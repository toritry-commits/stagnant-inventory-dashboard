-- ============================================================
-- 滞留在庫ダッシュボード用クエリ
-- ============================================================
-- 作成日: 2025-12-23
-- 更新日: 2025-12-23（リファクタリング実施：CTE統合・コード削減）
-- 目的: 滞留在庫の可視化と減損対策のためのデータ取得
--
-- リファクタリング内容:
--   - attribute_value_by_type: 属性タイプ別JOIN処理を共通化（5箇所→1箇所）
--   - sku_stock_ids_unified: 在庫id集約CTEを統合（10CTE→1CTE）
--
-- 主な機能:
--   1. SKU単位での滞留在庫情報（連番方式：1からMAX(SKU_ID)まで全行出力）
--   2. 滞留日数（平均、切り上げ）
--   3. 減損予定点数（動的四半期末：Q1/Q2/Q3/Q4）
--   4. 商品数（組み上げ分）/余りパーツ数の分離表示
--   5. 構成パーツ情報（パーツid、パーツ名、構成点数）
--   6. 在庫id詳細（減損対象の在庫idをパーツごとに表示）
--   7. 合計カラム（商品数、余りパーツ数、商品+余りパーツ）
--
-- 減損判定基準:
--   - 365日以上滞留で減損対象
--   - 四半期末: 2月末、5月末、8月末、11月末（会計期末: 2月末）
--
-- エリア別計算:
--   - 減損数は関東/関西/九州ごとに計算してから合計
--
-- 出力条件:
--   - 1からMAX(SKU_ID)までの全連番を出力
--   - SKU_ID昇順で並び替え
-- ============================================================

WITH
-- ============================================================
-- 0. SKU_IDの連番を生成（1からMAX(SKU_ID)まで）
-- ============================================================
max_sku_id AS (
  SELECT MAX(id) AS max_id FROM `lake.sku`
),
all_sku_ids AS (
  SELECT seq_id
  FROM max_sku_id, UNNEST(GENERATE_ARRAY(1, max_id)) AS seq_id
),

-- ============================================================
-- 0-1. 属性タイプ別のattribute_value（共通化：5箇所で再利用）
-- ============================================================
-- 各属性タイプ（Body/Leg/Mattress/MattressTopper/Guarantee）の
-- attribute_valueをproduct_id単位で取得する共通CTE
attribute_value_by_type AS (
  SELECT
    a.product_id,
    a.type,
    av.id AS av_id
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
),

-- ============================================================
-- 1. 四半期末日付の計算（動的）
-- ============================================================
-- 会計期末: 2月末、四半期末: 2月末、5月末、8月末、11月末
quarter_ends AS (
  SELECT
    -- 現在日付
    CURRENT_DATE() AS today,
    -- 現在の月
    EXTRACT(MONTH FROM CURRENT_DATE()) AS current_month,
    -- 現在の年
    EXTRACT(YEAR FROM CURRENT_DATE()) AS current_year,
    -- 直近の四半期末を計算
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 2 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 5 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 8 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 11 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30)
      ELSE
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28)
    END AS q1_end,
    -- 直近+1四半期末
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 2 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 5 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 8 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 11 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28)
      ELSE
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31)
    END AS q2_end,
    -- 直近+2四半期末
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 2 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 5 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 8 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 11 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31)
      ELSE
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31)
    END AS q3_end,
    -- 直近+3四半期末（11月末以降用）
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 2 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 5 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 8 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31)
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) <= 11 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31)
      ELSE
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 11, 30)
    END AS q4_end
),

-- ============================================================
-- 2. 引当可能な在庫（queries.sql準拠）
-- ============================================================
stocks AS (
  SELECT
    s.id,
    s.status,
    s.part_id,
    s.part_version_id,
    s.supplier_id,
    w.business_area,
    s.location_id,
    s.arrival_at,
    s.inspected_at,
    s.impairment_date  -- 減損予定日
  FROM `lake.stock` s
  INNER JOIN `lake.part` p ON p.id = s.part_id AND p.deleted_at IS NULL
  INNER JOIN `lake.part_version` pv ON pv.id = s.part_version_id AND pv.deleted_at IS NULL
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
  LEFT OUTER JOIN (
    SELECT ld.stock_id
    FROM `lake.lent` l INNER JOIN `lake.lent_detail` ld ON l.id = ld.lent_id
    WHERE l.status IN ('Preparing', 'Adjusting') AND l.deleted_at IS NULL AND ld.deleted_at IS NULL
    GROUP BY ld.stock_id
  ) to_c ON to_c.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT d.stock_id
    FROM `lake.lent_to_b` b INNER JOIN `lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing' AND b.deleted_at IS NULL AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT sm.stock_id
    FROM `lake.staff_memo` sm
    INNER JOIN `lake.staff_memo_tag` smt ON sm.id = smt.staff_memo_id AND sm.stock_id IS NOT NULL
    INNER JOIN `lake.memo_tag` mt ON mt.id = smt.memo_tag_id AND mt.id = 64
    WHERE sm.deleted_at IS NULL AND smt.deleted_at IS NULL AND mt.deleted_at IS NULL
    GROUP BY sm.stock_id
  ) tag ON tag.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT pds.stock_id
    FROM `lake.purchasing_detail_stock` pds
    INNER JOIN `lake.purchasing_detail` pd ON pds.purchasing_detail_id = pd.id
    INNER JOIN `lake.purchasing` p ON p.id = pd.purchasing_id
    WHERE pds.deleted_at IS NULL AND pd.deleted_at IS NULL AND p.deleted_at IS NULL
      AND p.status = 'Done' AND p.orderer_email = 'info+b-order@clas.style'
    GROUP BY pds.stock_id
  ) purchase ON purchase.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT xss.stock_id
    FROM `lake.external_sale_stock` xss
    INNER JOIN `lake.external_sale_product` xsp ON xsp.id = xss.external_sale_product_id
    WHERE xss.deleted_at IS NULL AND xsp.deleted_at IS NULL AND xsp.status != 'Deny'
    GROUP BY xss.stock_id
  ) external_sale ON external_sale.stock_id = s.id
  WHERE s.deleted_at IS NULL
    AND (s.status IN ('Ready', 'Waiting') OR (s.status = 'Recovery' AND s.part_id NOT IN (7108,7109,7110,7438,7439,7440,7570,7571,7572,7573)))
    AND tag.stock_id IS NULL
    AND to_c.stock_id IS NULL
    AND to_b.stock_id IS NULL
    AND w.available_for_business = TRUE
    AND s._rank_ NOT IN ('R', 'L')
    AND l.id NOT IN (5400,9389,9390,9521,9522,9523,9629,9702,9703,9951,10120,10355,10374,10415,10820,10948,11022)
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
    AND external_sale.stock_id IS NULL
),

-- ============================================================
-- 3. AttributeValue単位の在庫集計（queries.sql準拠）
-- ============================================================
avs AS (
  SELECT
    ANY_VALUE(a.product_id) AS product_id,
    ANY_VALUE(a.`type`) AS type,
    CONCAT(
      CASE ANY_VALUE(a.`type`)
        WHEN 'Body' THEN '本体'
        WHEN 'Leg' THEN '脚'
        WHEN 'Mattress' THEN 'マットレス'
        WHEN 'MattressTopper' THEN '寝心地オプション'
        WHEN 'Guarantee' THEN '補償'
      END,
      ': ', ANY_VALUE(av.value)) AS value,
    av.id,
    ANY_VALUE(av.status) AS status,
    IFNULL(MIN(avp.cnt_kanto), 0) AS cnt_kanto,
    IFNULL(MIN(avp.cnt_kansai), 0) AS cnt_kansai,
    IFNULL(MIN(avp.cnt_kyushu), 0) AS cnt_kyushu,
    IFNULL(MIN(avp.cnt_kanto_rec), 0) AS cnt_kanto_rec,
    IFNULL(MIN(avp.cnt_kansai_rec), 0) AS cnt_kansai_rec,
    IFNULL(MIN(avp.cnt_kyushu_rec), 0) AS cnt_kyushu_rec,
    ARRAY_AGG(DISTINCT supplier) AS suppliers,
    IFNULL(SUM(avp.cost), 0) AS cost
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  LEFT OUTER JOIN (
    SELECT
      ANY_VALUE(avp.attribute_value_id) AS av_id,
      CAST((IFNULL(ANY_VALUE(available_stock_kanto.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kanto,
      CAST((IFNULL(ANY_VALUE(available_stock_kansai.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kansai,
      CAST((IFNULL(ANY_VALUE(available_stock_kyushu.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kyushu,
      CAST((IFNULL(ANY_VALUE(available_stock_kanto.cnt), 0) + IFNULL(ANY_VALUE(available_recovery_stock_kanto.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kanto_rec,
      CAST((IFNULL(ANY_VALUE(available_stock_kansai.cnt), 0) + IFNULL(ANY_VALUE(available_recovery_stock_kansai.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kansai_rec,
      CAST((IFNULL(ANY_VALUE(available_stock_kyushu.cnt), 0) + IFNULL(ANY_VALUE(available_recovery_stock_kyushu.cnt), 0)) / ANY_VALUE(avp.quantity) AS INT64) AS cnt_kyushu_rec,
      ANY_VALUE(supplier.name) AS supplier,
      ANY_VALUE(part.cost) AS cost
    FROM `lake.attribute_value_part` avp
    INNER JOIN (
      SELECT p.id, IFNULL(instock.cost, IFNULL(outstock.cost, 0)) AS cost
      FROM `lake.part` p
      LEFT OUTER JOIN (SELECT part_id, MAX(cost) AS cost FROM `lake.stock` WHERE arrival_at != '9999-12-31' GROUP BY part_id) instock ON p.id = instock.part_id
      LEFT OUTER JOIN (SELECT part_id, MAX(cost) AS cost FROM `lake.stock` WHERE arrival_at = '9999-12-31' GROUP BY part_id) outstock ON p.id = outstock.part_id
    ) part ON part.id = avp.part_id
    INNER JOIN (
      SELECT pv.part_id, sup.name
      FROM `lake.part_version` pv
      INNER JOIN (SELECT pv.part_id, MAX(version) AS max_ver FROM `lake.part_version` pv GROUP BY pv.part_id) pv2 ON pv.version = pv2.max_ver AND pv.part_id = pv2.part_id
      INNER JOIN `lake.supplier` sup ON sup.id = pv.supplier_id
      WHERE pv.deleted_at IS NULL AND sup.deleted_at IS NULL
    ) supplier ON avp.part_id = supplier.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status IN ('Ready','Waiting') AND (ss.business_area = 'Kanto' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_stock_kanto ON available_stock_kanto.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND (ss.business_area = 'Kanto' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_recovery_stock_kanto ON available_recovery_stock_kanto.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status IN ('Ready','Waiting') AND (ss.business_area = 'Kansai' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_stock_kansai ON available_stock_kansai.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND (ss.business_area = 'Kansai' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_recovery_stock_kansai ON available_recovery_stock_kansai.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE status IN ('Ready','Waiting') AND (ss.business_area = 'Kyushu' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_stock_kyushu ON available_stock_kyushu.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND (ss.business_area = 'Kyushu' OR ss.business_area IS NULL) GROUP BY ss.part_id) available_recovery_stock_kyushu ON available_recovery_stock_kyushu.part_id = avp.part_id
    WHERE avp.deleted_at IS NULL
    GROUP BY avp.id
  ) avp ON avp.av_id = av.id
  GROUP BY av.id
),

-- ============================================================
-- 4. カテゴリ名の日本語変換マッピング
-- ============================================================
category_mapping AS (
  SELECT category_en, category_ja FROM UNNEST([
    STRUCT('Sofa' AS category_en, 'ソファ' AS category_ja),
    STRUCT('Bed', 'ベッド・寝具'),
    STRUCT('Chair', 'チェア'),
    STRUCT('WorkSeat', 'オフィスチェア'),
    STRUCT('Table', 'テーブル'),
    STRUCT('Dining', 'ダイニング'),
    STRUCT('Desk', 'デスク'),
    STRUCT('Storage', '収納'),
    STRUCT('TvBoard', 'テレビ台'),
    STRUCT('Lighting', '照明'),
    STRUCT('Fabric', 'ファブリック'),
    STRUCT('RugAndCarpet', 'ラグ・カーペット'),
    STRUCT('Curtain', 'カーテン'),
    STRUCT('KidsAndBabies', 'キッズ&ベビー'),
    STRUCT('InteriorGreen', '観葉植物'),
    STRUCT('Outdoor', 'アウトドア'),
    STRUCT('OtherFurniture', 'その他の家具'),
    STRUCT('Washer', '洗濯機'),
    STRUCT('Refrigerator', '冷蔵庫'),
    STRUCT('Microwave', '電子レンジ'),
    STRUCT('KitchenAppliance', 'キッチン家電'),
    STRUCT('Television', 'テレビ'),
    STRUCT('Cleaner', '掃除機'),
    STRUCT('PcPeripherals', 'PC周辺機器'),
    STRUCT('AirConditioning', '空調家電'),
    STRUCT('Beauty', '美容家電'),
    STRUCT('OtherElectronics', 'その他の家電'),
    STRUCT('Babycrib', 'ベビーベッド'),
    STRUCT('BabyBedding', 'ベビー寝具'),
    STRUCT('Mobile', 'モビール'),
    STRUCT('Bouncer', 'バウンサー'),
    STRUCT('BabyChair', 'ベビーチェア'),
    STRUCT('FamilyAppliance', 'ファミリー家電'),
    STRUCT('FamilyInterior', 'インテリア'),
    STRUCT('BabyCare', 'ベビーケア'),
    STRUCT('ChildSeat', 'チャイルドシート'),
    STRUCT('Fitness', 'フィットネス'),
    STRUCT('HighLowChair', 'ハイローチェア'),
    STRUCT('HomeGoods', 'ホームグッズ'),
    STRUCT('Mattress', 'マットレス'),
    STRUCT('OfficeChair', 'オフィスチェア'),
    STRUCT('OfficeDesk', 'オフィスデスク'),
    STRUCT('OfficeInterior', 'オフィスインテリア'),
    STRUCT('OfficePcPeripherals', 'オフィスPC周辺機器'),
    STRUCT('OfficeSofa', 'オフィスソファ'),
    STRUCT('OfficeStorage', 'オフィス収納'),
    STRUCT('OfficeTable', 'オフィステーブル'),
    STRUCT('Partition', 'パーティション'),
    STRUCT('Stroller', 'ベビーカー'),
    STRUCT('Travel', 'トラベル')
  ])
),

-- ============================================================
-- 5. 滞留日数計算
-- ============================================================
-- 5-1. 全在庫の「入庫日」を特定（資産台帳を優先）
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    COALESCE(MAX(fa.arrival_at), ANY_VALUE(s.arrival_at)) AS arrival_at
  FROM `lake.stock` s
  LEFT JOIN `finance.fixed_asset_register` fa ON s.id = fa.stock_id
  WHERE s.deleted_at IS NULL
  GROUP BY s.id, s.part_id
),

-- 5-2. 在庫ごとの「初回EC出荷日」
first_ec_ship AS (
  SELECT
    detail.stock_id,
    MIN(lent.shipped_at) AS first_shipped_at
  FROM `lake.lent` lent
  INNER JOIN `lake.lent_detail` detail ON lent.id = detail.lent_id
  WHERE lent.status NOT IN ('Cancel', 'PurchaseFailed')
    AND lent.shipped_at IS NOT NULL
    AND lent.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  GROUP BY detail.stock_id
),

-- 5-3. 在庫ごとの「初回法人出荷日」
first_b2b_ship AS (
  SELECT
    detail.stock_id,
    MIN(lent_b.start_at) AS first_start_at
  FROM `lake.lent_to_b` lent_b
  INNER JOIN `lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status NOT IN ('Cancel') AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL
  GROUP BY detail.stock_id
),

-- 5-4. 全履歴（C向け返却とB向け出荷/返却）を時系列で並べる
all_lent_history AS (
  SELECT detail.stock_id, lent_b.start_at AS event_date, 'B2B_Ship' AS event_type
  FROM `lake.lent_to_b` lent_b
  INNER JOIN `lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status NOT IN ('Cancel') AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  UNION ALL
  SELECT detail.stock_id, lent.return_at AS event_date, 'EC_Return' AS event_type
  FROM `lake.lent` lent
  INNER JOIN `lake.lent_detail` detail ON lent.id = detail.lent_id
  WHERE lent.status = 'Returned' AND lent.return_at IS NOT NULL AND lent.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  UNION ALL
  SELECT detail.stock_id, lent_b.end_at AS event_date, 'B2B_Return' AS event_type
  FROM `lake.lent_to_b` lent_b
  INNER JOIN `lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status = 'Ended' AND lent_b.end_at IS NOT NULL AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
),

-- 5-5. 現在倉庫在庫の滞留日数: 1周目（在庫中）- 新品入庫 -> 現在
retention_round_1_waiting AS (
  SELECT
    sa.part_id,
    sa.stock_id,
    DATE_DIFF(CURRENT_DATE(), CAST(sa.arrival_at AS DATE), DAY) AS duration_days
  FROM stock_arrivals sa
  INNER JOIN stocks st ON sa.stock_id = st.id
  LEFT JOIN first_b2b_ship b2b ON sa.stock_id = b2b.stock_id
  LEFT JOIN first_ec_ship ec ON sa.stock_id = ec.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND b2b.first_start_at IS NULL
    AND ec.first_shipped_at IS NULL
),

-- 5-6. 現在倉庫在庫の滞留日数: 2周目以降（在庫中）- 最終返却 -> 現在
retention_round_2_waiting AS (
  SELECT
    t.stock_id,
    DATE_DIFF(CURRENT_DATE(), t.event_date, DAY) AS duration_days
  FROM (
    SELECT
      stock_id, event_date, event_type,
      ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY event_date DESC) as rn
    FROM all_lent_history
  ) t
  INNER JOIN stocks st ON t.stock_id = st.id
  WHERE t.rn = 1
    AND t.event_type IN ('EC_Return', 'B2B_Return')
),

-- 5-7. 現在倉庫在庫の滞留日数をpart_id単位で集計
all_current_retention AS (
  SELECT part_id, duration_days FROM retention_round_1_waiting WHERE duration_days IS NOT NULL AND duration_days >= 0
  UNION ALL
  SELECT s.part_id, r2.duration_days FROM retention_round_2_waiting r2 INNER JOIN `lake.stock` s ON r2.stock_id = s.id WHERE r2.duration_days IS NOT NULL AND r2.duration_days >= 0
),

part_current_retention_avg AS (
  SELECT
    part_id,
    AVG(duration_days) as avg_current_retention_days
  FROM all_current_retention
  GROUP BY part_id
),

-- 5-8. SKU→part_idマッピング（SKUテーブルのhashを使用）
-- ※SKU.hashとattribute_valueの組み合わせから計算したhashが一致するもののみ
-- ※attribute_value_by_type CTEを使用して共通化
sku_part_mapping AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    avp.part_id
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = pd.id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = pd.id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = pd.id AND mrt.type = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr ON gr.product_id = pd.id AND gr.type = 'Guarantee'
  CROSS JOIN UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id AND avp.deleted_at IS NULL
  WHERE sku.deleted_at IS NULL
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element), ',')))
    AND av_id IS NOT NULL
  GROUP BY sku.id, sku.hash, avp.part_id
),

-- 5-9. SKUごとの現在の平均滞留日数（倉庫在庫ベース）
sku_current_retention_avg AS (
  SELECT
    spm.sku_hash,
    ROUND(AVG(pcra.avg_current_retention_days), 1) AS avg_current_retention_days
  FROM sku_part_mapping spm
  LEFT JOIN part_current_retention_avg pcra ON spm.part_id = pcra.part_id
  GROUP BY spm.sku_hash
),

-- ============================================================
-- 6. 在庫ごとの滞留開始日を特定（減損計算用）- エリア情報付き
-- ============================================================
-- 滞留開始日 = 入庫日（1周目）または最終返却日（2周目以降）
stock_retention_start AS (
  -- 1周目: 新品入庫→現在在庫中（一度も出荷されていない）
  SELECT
    sa.stock_id,
    s.part_id,
    s.business_area,
    s.impairment_date,  -- 減損日（登録済みの場合）
    CAST(sa.arrival_at AS DATE) AS retention_start_date
  FROM stock_arrivals sa
  INNER JOIN stocks s ON sa.stock_id = s.id
  LEFT JOIN first_b2b_ship b2b ON sa.stock_id = b2b.stock_id
  LEFT JOIN first_ec_ship ec ON sa.stock_id = ec.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND b2b.first_start_at IS NULL
    AND ec.first_shipped_at IS NULL

  UNION ALL

  -- 2周目以降: 最終返却日→現在在庫中
  SELECT
    t.stock_id,
    s.part_id,
    s.business_area,
    s.impairment_date,  -- 減損日（登録済みの場合）
    CAST(t.event_date AS DATE) AS retention_start_date
  FROM (
    SELECT
      stock_id, event_date, event_type,
      ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY event_date DESC) as rn
    FROM all_lent_history
  ) t
  INNER JOIN stocks s ON t.stock_id = s.id
  WHERE t.rn = 1
    AND t.event_type IN ('EC_Return', 'B2B_Return')
),

-- ============================================================
-- 6-2. SKUごとのパーツ構成とquantity情報
-- ============================================================
-- ※attribute_value_by_type CTEを使用して共通化
sku_part_detail AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    avp.part_id,
    p.name AS part_name,
    a.type AS part_type,
    avp.quantity
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = pd.id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = pd.id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = pd.id AND mrt.type = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr ON gr.product_id = pd.id AND gr.type = 'Guarantee'
  CROSS JOIN UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id AND avp.deleted_at IS NULL
  INNER JOIN `lake.attribute_value` av ON av.id = avp.attribute_value_id AND av.deleted_at IS NULL
  INNER JOIN `lake.attribute` a ON a.id = av.attribute_id AND a.deleted_at IS NULL
  INNER JOIN `lake.part` p ON p.id = avp.part_id AND p.deleted_at IS NULL
  WHERE sku.deleted_at IS NULL
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element), ',')))
    AND av_id IS NOT NULL
),

-- ============================================================
-- 6-3. SKUごとの構成パーツ情報（改行区切り）
-- ============================================================
sku_part_info AS (
  SELECT
    sku_hash,
    STRING_AGG(CAST(part_id AS STRING), '\n' ORDER BY part_id) AS part_ids_str,
    STRING_AGG(part_name, '\n' ORDER BY part_id) AS part_names_str,
    STRING_AGG(CAST(quantity AS STRING), '\n' ORDER BY part_id) AS quantities_str
  FROM sku_part_detail
  GROUP BY sku_hash
),

-- ============================================================
-- 6-4. エリア・パーツ・SKU別の在庫情報（減損計算用）
-- ============================================================
-- パーツ・エリア・SKU別に在庫を集計（減損判定情報付き）
stock_with_impairment_info AS (
  SELECT
    srs.stock_id,
    srs.part_id,
    srs.business_area,
    srs.retention_start_date,
    qe.q1_end,
    qe.q2_end,
    qe.q3_end,
    qe.q4_end,
    -- 各時点での減損判定
    -- is_impaired: 既に減損日が登録されている場合、または滞留365日以上の場合
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN TRUE
      WHEN DATE_DIFF(CURRENT_DATE(), srs.retention_start_date, DAY) >= 365 THEN TRUE
      ELSE FALSE
    END AS is_impaired,
    CASE WHEN DATE_DIFF(qe.q1_end, srs.retention_start_date, DAY) >= 365 THEN TRUE ELSE FALSE END AS is_q1_impaired,
    CASE WHEN DATE_DIFF(qe.q2_end, srs.retention_start_date, DAY) >= 365 THEN TRUE ELSE FALSE END AS is_q2_impaired,
    CASE WHEN DATE_DIFF(qe.q3_end, srs.retention_start_date, DAY) >= 365 THEN TRUE ELSE FALSE END AS is_q3_impaired,
    CASE WHEN DATE_DIFF(qe.q4_end, srs.retention_start_date, DAY) >= 365 THEN TRUE ELSE FALSE END AS is_q4_impaired
  FROM stock_retention_start srs
  CROSS JOIN quarter_ends qe
),

-- ============================================================
-- 6-5. SKU・エリア・パーツタイプ別の在庫数とquantity
-- ============================================================
sku_area_part_stock AS (
  SELECT
    spd.sku_hash,
    spd.part_id,
    spd.part_type,
    spd.quantity,
    COALESCE(swii.business_area, 'Kanto') AS business_area,
    COUNT(swii.stock_id) AS stock_cnt,
    -- 各期間の減損在庫数（累計）
    COUNT(CASE WHEN swii.is_impaired THEN swii.stock_id END) AS impaired_cnt,
    COUNT(CASE WHEN swii.is_q1_impaired THEN swii.stock_id END) AS q1_impaired_cnt,
    COUNT(CASE WHEN swii.is_q2_impaired THEN swii.stock_id END) AS q2_impaired_cnt,
    COUNT(CASE WHEN swii.is_q3_impaired THEN swii.stock_id END) AS q3_impaired_cnt,
    COUNT(CASE WHEN swii.is_q4_impaired THEN swii.stock_id END) AS q4_impaired_cnt,
    -- 各期間のみの減損在庫数（前期間・減損済み除外）
    COUNT(CASE WHEN swii.is_q1_impaired AND NOT swii.is_impaired THEN swii.stock_id END) AS q1_only_cnt,
    COUNT(CASE WHEN swii.is_q2_impaired AND NOT swii.is_q1_impaired AND NOT swii.is_impaired THEN swii.stock_id END) AS q2_only_cnt,
    COUNT(CASE WHEN swii.is_q3_impaired AND NOT swii.is_q2_impaired AND NOT swii.is_impaired THEN swii.stock_id END) AS q3_only_cnt,
    COUNT(CASE WHEN swii.is_q4_impaired AND NOT swii.is_q3_impaired AND NOT swii.is_impaired THEN swii.stock_id END) AS q4_only_cnt
  FROM sku_part_detail spd
  LEFT JOIN stock_with_impairment_info swii ON spd.part_id = swii.part_id
  GROUP BY spd.sku_hash, spd.part_id, spd.part_type, spd.quantity, swii.business_area
),

-- ============================================================
-- 6-6. SKU・エリア別の組み上げ可能数を計算
-- ============================================================
sku_area_assemblable AS (
  SELECT
    sku_hash,
    business_area,
    -- 各パーツタイプの在庫数÷quantityのMINが組み上げ可能数
    MIN(CAST(stock_cnt / quantity AS INT64)) AS assemblable_cnt,
    -- 減損済みの組み上げ可能数
    MIN(CAST(impaired_cnt / quantity AS INT64)) AS assemblable_impaired,
    -- Q1時点の組み上げ可能数
    MIN(CAST(q1_impaired_cnt / quantity AS INT64)) AS assemblable_q1,
    -- Q2時点の組み上げ可能数
    MIN(CAST(q2_impaired_cnt / quantity AS INT64)) AS assemblable_q2,
    -- Q3時点の組み上げ可能数
    MIN(CAST(q3_impaired_cnt / quantity AS INT64)) AS assemblable_q3,
    -- Q4時点の組み上げ可能数
    MIN(CAST(q4_impaired_cnt / quantity AS INT64)) AS assemblable_q4,
    -- 各期間のみの組み上げ可能数（前期間・減損済み除外）
    MIN(CAST(q1_only_cnt / quantity AS INT64)) AS assemblable_q1_only,
    MIN(CAST(q2_only_cnt / quantity AS INT64)) AS assemblable_q2_only,
    MIN(CAST(q3_only_cnt / quantity AS INT64)) AS assemblable_q3_only,
    MIN(CAST(q4_only_cnt / quantity AS INT64)) AS assemblable_q4_only
  FROM sku_area_part_stock
  GROUP BY sku_hash, business_area
),

-- ============================================================
-- 6-7. SKU別のエリア合計（組み上げ数・減損数）
-- ============================================================
sku_impairment_summary AS (
  SELECT
    sku_hash,
    -- 組み上げ可能数（エリア合計）
    SUM(assemblable_cnt) AS total_assemblable,
    -- 減損済み（商品数）
    SUM(assemblable_impaired) AS impaired_product_cnt,
    -- Q1減損予定（商品数・累計）
    SUM(assemblable_q1) AS q1_product_cumulative,
    -- Q2減損予定（商品数・累計）
    SUM(assemblable_q2) AS q2_product_cumulative,
    -- Q3減損予定（商品数・累計）
    SUM(assemblable_q3) AS q3_product_cumulative,
    -- Q4減損予定（商品数・累計）
    SUM(assemblable_q4) AS q4_product_cumulative
  FROM sku_area_assemblable
  GROUP BY sku_hash
),

-- ============================================================
-- 6-8. SKU・エリア・パーツ別の余り在庫数を計算
-- ============================================================
sku_area_leftover AS (
  SELECT
    saps.sku_hash,
    saps.part_id,
    saps.part_type,
    saps.quantity,
    saps.business_area,
    saps.stock_cnt,
    saa.assemblable_cnt,
    -- 余りパーツ数 = 在庫数 - (組み上げ可能数 × quantity)
    GREATEST(0, saps.stock_cnt - (saa.assemblable_cnt * saps.quantity)) AS leftover_cnt,
    -- 減損済み余りパーツ
    GREATEST(0, saps.impaired_cnt - (saa.assemblable_impaired * saps.quantity)) AS leftover_impaired,
    -- Q1減損予定余りパーツ（累計：後方互換用に残す）
    GREATEST(0, saps.q1_impaired_cnt - (saa.assemblable_q1 * saps.quantity)) AS leftover_q1,
    -- Q2減損予定余りパーツ（累計：後方互換用に残す）
    GREATEST(0, saps.q2_impaired_cnt - (saa.assemblable_q2 * saps.quantity)) AS leftover_q2,
    -- Q3減損予定余りパーツ（累計：後方互換用に残す）
    GREATEST(0, saps.q3_impaired_cnt - (saa.assemblable_q3 * saps.quantity)) AS leftover_q3,
    -- Q4減損予定余りパーツ（累計：後方互換用に残す）
    GREATEST(0, saps.q4_impaired_cnt - (saa.assemblable_q4 * saps.quantity)) AS leftover_q4,
    -- 各期間のみの余りパーツ（前期間・減損済み除外）
    GREATEST(0, saps.q1_only_cnt - (saa.assemblable_q1_only * saps.quantity)) AS leftover_q1_only,
    GREATEST(0, saps.q2_only_cnt - (saa.assemblable_q2_only * saps.quantity)) AS leftover_q2_only,
    GREATEST(0, saps.q3_only_cnt - (saa.assemblable_q3_only * saps.quantity)) AS leftover_q3_only,
    GREATEST(0, saps.q4_only_cnt - (saa.assemblable_q4_only * saps.quantity)) AS leftover_q4_only
  FROM sku_area_part_stock saps
  INNER JOIN sku_area_assemblable saa
    ON saps.sku_hash = saa.sku_hash AND saps.business_area = saa.business_area
),

-- ============================================================
-- 6-9. SKU別の余りパーツ合計
-- ============================================================
sku_leftover_summary AS (
  SELECT
    sku_hash,
    -- 余りパーツ数の合計
    SUM(leftover_cnt) AS total_leftover,
    -- 減損済み余りパーツ
    SUM(leftover_impaired) AS leftover_impaired_total,
    -- Q1減損予定余りパーツ（累計：後方互換用）
    SUM(leftover_q1) AS leftover_q1_cumulative,
    -- Q2減損予定余りパーツ（累計：後方互換用）
    SUM(leftover_q2) AS leftover_q2_cumulative,
    -- Q3減損予定余りパーツ（累計：後方互換用）
    SUM(leftover_q3) AS leftover_q3_cumulative,
    -- Q4減損予定余りパーツ（累計：後方互換用）
    SUM(leftover_q4) AS leftover_q4_cumulative,
    -- 各期間のみの余りパーツ合計（前期間・減損済み除外）
    SUM(leftover_q1_only) AS leftover_q1_only_total,
    SUM(leftover_q2_only) AS leftover_q2_only_total,
    SUM(leftover_q3_only) AS leftover_q3_only_total,
    SUM(leftover_q4_only) AS leftover_q4_only_total
  FROM sku_area_leftover
  GROUP BY sku_hash
),

-- ============================================================
-- 6-10. 減損予定の在庫id一覧を取得（組み上げ対象/余り別）
-- ============================================================
-- 各減損カテゴリ内での組み上げ判定を行う
-- ※「組み上げ対象」= その減損カテゴリ内で組み上げ可能な商品分として使用される在庫
stock_with_category_order AS (
  SELECT
    spd.sku_hash,
    spd.part_id,
    spd.part_type,
    spd.quantity,
    swii.stock_id,
    COALESCE(swii.business_area, 'Kanto') AS business_area,
    swii.retention_start_date,
    swii.is_impaired,
    swii.is_q1_impaired,
    swii.is_q2_impaired,
    swii.is_q3_impaired,
    swii.is_q4_impaired,
    -- 各減損カテゴリ内でstock_id昇順の番号を振る（エリア・パーツ別）
    -- 減損済み在庫内での順番
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, spd.part_id, swii.business_area,
        CASE WHEN swii.is_impaired THEN 1 ELSE 0 END
      ORDER BY swii.stock_id
    ) AS impaired_order,
    -- Q1減損予定在庫（減損済み除く）内での順番
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, spd.part_id, swii.business_area,
        CASE WHEN swii.is_q1_impaired AND NOT swii.is_impaired THEN 1 ELSE 0 END
      ORDER BY swii.stock_id
    ) AS q1_order,
    -- Q2減損予定在庫（Q1除く）内での順番
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, spd.part_id, swii.business_area,
        CASE WHEN swii.is_q2_impaired AND NOT swii.is_q1_impaired THEN 1 ELSE 0 END
      ORDER BY swii.stock_id
    ) AS q2_order,
    -- Q3減損予定在庫（Q2除く）内での順番
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, spd.part_id, swii.business_area,
        CASE WHEN swii.is_q3_impaired AND NOT swii.is_q2_impaired THEN 1 ELSE 0 END
      ORDER BY swii.stock_id
    ) AS q3_order,
    -- Q4減損予定在庫（Q3除く）内での順番
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, spd.part_id, swii.business_area,
        CASE WHEN swii.is_q4_impaired AND NOT swii.is_q3_impaired THEN 1 ELSE 0 END
      ORDER BY swii.stock_id
    ) AS q4_order
  FROM sku_part_detail spd
  INNER JOIN stock_with_impairment_info swii ON spd.part_id = swii.part_id
),

-- 各減損カテゴリ内での組み上げ対象かどうかを判定
stock_with_category_assembly_flag AS (
  SELECT
    swo.*,
    saa.assemblable_impaired,
    saa.assemblable_q1,
    saa.assemblable_q2,
    saa.assemblable_q3,
    saa.assemblable_q4,
    -- 減損済み在庫: 組み上げ対象判定
    CASE
      WHEN swo.is_impaired AND swo.impaired_order <= saa.assemblable_impaired * swo.quantity THEN TRUE
      ELSE FALSE
    END AS is_impaired_assembled,
    -- Q1減損予定在庫: 組み上げ対象判定（Q1差分 = assemblable_q1 - assemblable_impaired）
    CASE
      WHEN swo.is_q1_impaired AND NOT swo.is_impaired
        AND swo.q1_order <= GREATEST(0, saa.assemblable_q1 - saa.assemblable_impaired) * swo.quantity THEN TRUE
      ELSE FALSE
    END AS is_q1_assembled,
    -- Q2減損予定在庫: 組み上げ対象判定（Q2差分 = assemblable_q2 - assemblable_q1）
    -- ※既に減損済み（is_impaired=TRUE）の在庫は除外する
    CASE
      WHEN swo.is_q2_impaired AND NOT swo.is_q1_impaired AND NOT swo.is_impaired
        AND swo.q2_order <= GREATEST(0, saa.assemblable_q2 - saa.assemblable_q1) * swo.quantity THEN TRUE
      ELSE FALSE
    END AS is_q2_assembled,
    -- Q3減損予定在庫: 組み上げ対象判定（Q3差分 = assemblable_q3 - assemblable_q2）
    -- ※既に減損済み（is_impaired=TRUE）の在庫は除外する
    CASE
      WHEN swo.is_q3_impaired AND NOT swo.is_q2_impaired AND NOT swo.is_impaired
        AND swo.q3_order <= GREATEST(0, saa.assemblable_q3 - saa.assemblable_q2) * swo.quantity THEN TRUE
      ELSE FALSE
    END AS is_q3_assembled,
    -- Q4減損予定在庫: 組み上げ対象判定（Q4差分 = assemblable_q4 - assemblable_q3）
    -- ※既に減損済み（is_impaired=TRUE）の在庫は除外する
    CASE
      WHEN swo.is_q4_impaired AND NOT swo.is_q3_impaired AND NOT swo.is_impaired
        AND swo.q4_order <= GREATEST(0, saa.assemblable_q4 - saa.assemblable_q3) * swo.quantity THEN TRUE
      ELSE FALSE
    END AS is_q4_assembled
  FROM stock_with_category_order swo
  LEFT JOIN sku_area_assemblable saa
    ON swo.sku_hash = saa.sku_hash AND swo.business_area = saa.business_area
),

-- ============================================================
-- 6-11. SKU別の在庫id詳細（統合CTE：10個のCTEを1つに集約）
-- ============================================================
-- 在庫タイプ（組み上げ/余り）×期間（減損済/Q1/Q2/Q3/Q4）の組み合わせを1つのCTEで処理
-- ※各カテゴリ内で「組み上げ可能な商品数分」の在庫IDのみを「商品」として表示
-- ※パーツごとに1行にまとめる（同じパーツIDで複数行に分かれないようにする）
sku_stock_ids_unified AS (
  SELECT
    sku_hash,
    -- 組み上げ対象の在庫id（各カテゴリの組み上げ可能数分のみ）
    STRING_AGG(CASE WHEN impaired_assembled_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', impaired_assembled_ids) END, '\n' ORDER BY part_id) AS impaired_assembled_ids,
    STRING_AGG(CASE WHEN q1_assembled_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q1_assembled_ids) END, '\n' ORDER BY part_id) AS q1_assembled_ids,
    STRING_AGG(CASE WHEN q2_assembled_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q2_assembled_ids) END, '\n' ORDER BY part_id) AS q2_assembled_ids,
    STRING_AGG(CASE WHEN q3_assembled_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q3_assembled_ids) END, '\n' ORDER BY part_id) AS q3_assembled_ids,
    STRING_AGG(CASE WHEN q4_assembled_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q4_assembled_ids) END, '\n' ORDER BY part_id) AS q4_assembled_ids,
    -- 余りパーツの在庫id（各カテゴリで組み上げ対象外のもの）
    STRING_AGG(CASE WHEN impaired_leftover_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', impaired_leftover_ids) END, '\n' ORDER BY part_id) AS impaired_leftover_ids,
    STRING_AGG(CASE WHEN q1_leftover_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q1_leftover_ids) END, '\n' ORDER BY part_id) AS q1_leftover_ids,
    STRING_AGG(CASE WHEN q2_leftover_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q2_leftover_ids) END, '\n' ORDER BY part_id) AS q2_leftover_ids,
    STRING_AGG(CASE WHEN q3_leftover_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q3_leftover_ids) END, '\n' ORDER BY part_id) AS q3_leftover_ids,
    STRING_AGG(CASE WHEN q4_leftover_ids IS NOT NULL THEN CONCAT('[', CAST(part_id AS STRING), ']', q4_leftover_ids) END, '\n' ORDER BY part_id) AS q4_leftover_ids
  FROM (
    -- パーツ単位で各カテゴリの在庫IDを集約
    SELECT
      sku_hash,
      part_id,
      -- 減損済み（組み上げ対象）
      STRING_AGG(CASE WHEN is_impaired_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS impaired_assembled_ids,
      -- Q1（組み上げ対象）
      STRING_AGG(CASE WHEN is_q1_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q1_assembled_ids,
      -- Q2（組み上げ対象）
      STRING_AGG(CASE WHEN is_q2_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q2_assembled_ids,
      -- Q3（組み上げ対象）
      STRING_AGG(CASE WHEN is_q3_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q3_assembled_ids,
      -- Q4（組み上げ対象）
      STRING_AGG(CASE WHEN is_q4_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q4_assembled_ids,
      -- 減損済み（余りパーツ）
      STRING_AGG(CASE WHEN is_impaired AND NOT is_impaired_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS impaired_leftover_ids,
      -- Q1（余りパーツ）: 減損済みは除外
      STRING_AGG(CASE WHEN is_q1_impaired AND NOT is_impaired AND NOT is_q1_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q1_leftover_ids,
      -- Q2（余りパーツ）: Q1以前と減損済みは除外
      STRING_AGG(CASE WHEN is_q2_impaired AND NOT is_q1_impaired AND NOT is_impaired AND NOT is_q2_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q2_leftover_ids,
      -- Q3（余りパーツ）: Q2以前と減損済みは除外
      STRING_AGG(CASE WHEN is_q3_impaired AND NOT is_q2_impaired AND NOT is_impaired AND NOT is_q3_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q3_leftover_ids,
      -- Q4（余りパーツ）: Q3以前と減損済みは除外
      STRING_AGG(CASE WHEN is_q4_impaired AND NOT is_q3_impaired AND NOT is_impaired AND NOT is_q4_assembled THEN CAST(stock_id AS STRING) END, ', ' ORDER BY stock_id) AS q4_leftover_ids
    FROM stock_with_category_assembly_flag
    GROUP BY sku_hash, part_id
  )
  GROUP BY sku_hash
),

-- ============================================================
-- 7. パーツ単位の在庫数・パーツタイプ情報
-- ============================================================
part_stock_with_type AS (
  SELECT
    avp.part_id,
    a.type AS part_type,
    -- リカバリー込み在庫数
    COUNT(CASE WHEN (st.business_area = 'Kanto' OR st.business_area IS NULL) THEN st.id END) AS cnt_kanto_rec,
    COUNT(CASE WHEN (st.business_area = 'Kansai' OR st.business_area IS NULL) THEN st.id END) AS cnt_kansai_rec,
    COUNT(CASE WHEN (st.business_area = 'Kyushu' OR st.business_area IS NULL) THEN st.id END) AS cnt_kyushu_rec
  FROM `lake.attribute_value_part` avp
  INNER JOIN `lake.attribute_value` av ON avp.attribute_value_id = av.id AND av.deleted_at IS NULL
  INNER JOIN `lake.attribute` a ON av.attribute_id = a.id AND a.deleted_at IS NULL
  LEFT JOIN stocks st ON st.part_id = avp.part_id
  WHERE avp.deleted_at IS NULL
  GROUP BY avp.part_id, a.type
),

-- ============================================================
-- 8. 重複SKU除外判定（queries.sql準拠）
-- ============================================================
-- 現在の属性構成から計算したhashのリスト（孤立SKU判定用）
-- ※attribute_value_by_type CTEを使用して共通化
current_attribute_hashes AS (
  SELECT DISTINCT
    TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element), ','))) AS sku_hash
  FROM `lake.series` se
  INNER JOIN `lake.product` pd ON pd.series_id = se.id AND se.deleted_at IS NULL AND pd.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = pd.id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = pd.id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = pd.id AND mrt.type = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr ON gr.product_id = pd.id AND gr.type = 'Guarantee'
),

-- SKUテーブルを起点とした商品情報（削除済みSKUも含む）
sku_product_info AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    sku.deleted_at AS sku_deleted_at,
    pd.id AS product_id,
    se.name AS series_name,
    pd.name AS product_name,
    br.backyard_name AS brand_name,
    se.category AS category,
    pd.retail_price,
    pc.customer AS customer,
    CASE WHEN sku.deleted_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_deleted_sku,
    CASE WHEN cah.sku_hash IS NOT NULL THEN FALSE ELSE TRUE END AS is_orphan_sku
  FROM `lake.sku` sku
  LEFT JOIN `lake.product` pd ON pd.id = sku.product_id
  LEFT JOIN `lake.series` se ON se.id = pd.series_id
  LEFT JOIN `lake.brand` br ON br.id = se.brand_id
  LEFT JOIN (
    SELECT product_id, STRING_AGG(CASE customer WHEN 'Consumer' THEN '一般顧客' WHEN 'BusinessSmallOffice' THEN '法人顧客' END) AS customer
    FROM `lake.product_customer` WHERE deleted_at IS NULL
    GROUP BY product_id
  ) pc ON pc.product_id = pd.id
  LEFT JOIN current_attribute_hashes cah ON cah.sku_hash = sku.hash
),

-- 現在の属性構成に基づく在庫情報（hashをキーに）
attribute_based_info AS (
  SELECT
    TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.id, leg.id, mr.id, mrt.id, gr.id]) AS element ORDER BY element), ','))) AS sku_hash,
    ARRAY_TO_STRING([body.value, leg.value, mr.value, mrt.value, gr.value], ' ') AS value,
    (SELECT STRING_AGG(DISTINCT suppliers) FROM UNNEST(ARRAY_CONCAT(IFNULL(body.suppliers, []), IFNULL(leg.suppliers, []), IFNULL(mr.suppliers, []), IFNULL(mrt.suppliers, []), IFNULL(gr.suppliers, []))) suppliers) AS suppliers,
    IFNULL(body.cost, 0) + IFNULL(leg.cost, 0) + IFNULL(mr.cost, 0) AS cost,
    -- 四半期末日付
    body.q1_end AS q1_end,
    body.q2_end AS q2_end,
    body.q3_end AS q3_end
  FROM `lake.series` se
  INNER JOIN `lake.product` pd ON pd.series_id = se.id AND se.deleted_at IS NULL AND pd.deleted_at IS NULL
  LEFT OUTER JOIN (
    SELECT avs.product_id, avs.`type`, avs.value, avs.id, avs.status, avs.suppliers, avs.cost,
           avs.cnt_kanto_rec, avs.cnt_kansai_rec, avs.cnt_kyushu_rec,
           qe.q1_end, qe.q2_end, qe.q3_end
    FROM avs
    CROSS JOIN quarter_ends qe
    WHERE avs.`type` = 'Body'
  ) body ON body.product_id = pd.id
  LEFT OUTER JOIN (
    SELECT avs.product_id, avs.`type`, avs.value, avs.id, avs.status, avs.suppliers, avs.cost,
           avs.cnt_kanto_rec, avs.cnt_kansai_rec, avs.cnt_kyushu_rec
    FROM avs
    WHERE avs.`type` = 'Leg'
  ) leg ON leg.product_id = pd.id
  LEFT OUTER JOIN (
    SELECT avs.product_id, avs.`type`, avs.value, avs.id, avs.status, avs.suppliers, avs.cost,
           avs.cnt_kanto_rec, avs.cnt_kansai_rec, avs.cnt_kyushu_rec
    FROM avs
    WHERE avs.`type` = 'Mattress'
  ) mr ON mr.product_id = pd.id
  LEFT OUTER JOIN (
    SELECT avs.product_id, avs.`type`, avs.value, avs.id, avs.status, avs.suppliers, avs.cost,
           avs.cnt_kanto_rec, avs.cnt_kansai_rec, avs.cnt_kyushu_rec
    FROM avs
    WHERE avs.`type` = 'MattressTopper'
  ) mrt ON mrt.product_id = pd.id
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto_rec, cnt_kansai_rec, cnt_kyushu_rec FROM avs WHERE `type` = 'Guarantee') gr ON gr.product_id = pd.id
),

-- 重複SKU判定用
-- 6-1. SKUとattribute_valueの関係を取得（SKUテーブル起点）
-- ※attribute_value_by_type CTEを使用して共通化
dup_sku_attribute_mapping AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    pd.id AS product_id,
    pd.name AS product_name,
    se.name AS series_name,
    body.av_id AS body_av_id,
    leg.av_id AS leg_av_id,
    mr.av_id AS mattress_av_id,
    mrt.av_id AS mattress_topper_av_id,
    gr.av_id AS guarantee_av_id
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  INNER JOIN `lake.series` se ON se.id = pd.series_id AND se.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = pd.id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = pd.id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = pd.id AND mrt.type = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr ON gr.product_id = pd.id AND gr.type = 'Guarantee'
  WHERE sku.deleted_at IS NULL
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element), ',')))
),

-- 6-2. SKUごとにpart_idの配列を取得（補償を除く）
dup_sku_with_parts AS (
  SELECT
    sam.sku_id,
    sam.sku_hash,
    sam.product_id,
    sam.product_name,
    sam.series_name,
    sam.guarantee_av_id,
    -- part_idをソートして文字列化（比較用キー）
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT CAST(avp.part_id AS STRING) ORDER BY CAST(avp.part_id AS STRING)),
      ','
    ) AS part_id_key,
    ARRAY_AGG(DISTINCT avp.part_id ORDER BY avp.part_id) AS part_ids
  FROM dup_sku_attribute_mapping sam
  -- 補償(guarantee)を除いたattribute_value_idをUNNEST
  CROSS JOIN UNNEST([sam.body_av_id, sam.leg_av_id, sam.mattress_av_id, sam.mattress_topper_av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp
    ON avp.attribute_value_id = av_id
    AND avp.deleted_at IS NULL
  WHERE av_id IS NOT NULL
  GROUP BY sam.sku_id, sam.sku_hash, sam.product_id, sam.product_name, sam.series_name, sam.guarantee_av_id
),

-- 6-3. 補償情報を取得
dup_guarantee_info AS (
  SELECT
    av.id AS guarantee_av_id,
    CONCAT('補償: ', av.value) AS guarantee_name,
    CASE
      WHEN av.value LIKE '%汚損補償 付き%' THEN TRUE
      ELSE FALSE
    END AS has_damage_guarantee
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.type = 'Guarantee'
    AND a.deleted_at IS NULL
    AND av.deleted_at IS NULL
),

-- 6-4. SKUに補償情報を結合
dup_sku_with_guarantee AS (
  SELECT
    swp.*,
    IFNULL(gi.guarantee_name, '補償なし') AS guarantee_name,
    IFNULL(gi.has_damage_guarantee, FALSE) AS has_damage_guarantee
  FROM dup_sku_with_parts swp
  LEFT JOIN dup_guarantee_info gi ON gi.guarantee_av_id = swp.guarantee_av_id
),

-- 6-5. サプライヤー情報を取得（part_version経由）
dup_supplier_info AS (
  SELECT
    swg.sku_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT sup.name), ', ') AS suppliers
  FROM dup_sku_with_guarantee swg
  CROSS JOIN UNNEST(swg.part_ids) AS part_id
  INNER JOIN `lake.part_version` pv ON pv.part_id = part_id AND pv.deleted_at IS NULL
  INNER JOIN (
    SELECT pv2.part_id, MAX(pv2.version) AS max_ver
    FROM `lake.part_version` pv2
    GROUP BY pv2.part_id
  ) pv_max ON pv.part_id = pv_max.part_id AND pv.version = pv_max.max_ver
  INNER JOIN `lake.supplier` sup ON sup.id = pv.supplier_id AND sup.deleted_at IS NULL
  GROUP BY swg.sku_id
),

-- 6-6. SKUにサプライヤー情報を結合
dup_sku_full_info AS (
  SELECT
    swg.*,
    IFNULL(si.suppliers, '') AS suppliers,
    CASE
      WHEN IFNULL(si.suppliers, '') LIKE '%法人小物管理用%' THEN TRUE
      ELSE FALSE
    END AS is_corporate_item,
    -- セット品フラグ（商品名に「セット」が含まれる場合）
    -- ただし、特定のセット名（増連セット、片面開閉カバーセット、ハコ4色セット、ジョイントセット、木インセットパネル）は除外対象外
    CASE
      WHEN swg.product_name LIKE '%セット%'
        AND swg.product_name NOT LIKE '%増連セット%'
        AND swg.product_name NOT LIKE '%片面開閉カバーセット%'
        AND swg.product_name NOT LIKE '%ハコ4色セット%'
        AND swg.product_name NOT LIKE '%ジョイントセット%'
        AND swg.product_name NOT LIKE '%インセットパネル%'
      THEN TRUE
      ELSE FALSE
    END AS is_set_item
  FROM dup_sku_with_guarantee swg
  LEFT JOIN dup_supplier_info si ON si.sku_id = swg.sku_id
),

-- 6-7. part_idの組み合わせが重複しているグループを抽出
-- ※セット品・CLAS SET・おまかせ等は既に別条件で除外されるため、重複判定から除外
dup_duplicate_groups AS (
  SELECT
    part_id_key,
    COUNT(*) AS sku_count
  FROM dup_sku_full_info
  WHERE is_corporate_item = FALSE  -- 法人小物管理用は重複判定から除外
    AND is_set_item = FALSE        -- セット品は重複判定から除外
    AND series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND series_name NOT LIKE '%【CLAS SET】%'
    AND series_name NOT LIKE '%エイトレント%'
    AND product_name NOT LIKE '%プラン%'
  GROUP BY part_id_key
  HAVING COUNT(*) > 1  -- 2つ以上のSKUが同じpart_id組み合わせを持つ
),

-- 6-8. 重複グループに属するSKUの詳細を出力
-- ※セット品・CLAS SET・おまかせ等は既に別条件で除外されるため、重複判定から除外
dup_ranked_duplicates AS (
  SELECT
    sfi.sku_id,
    ROW_NUMBER() OVER (
      PARTITION BY sfi.part_id_key
      ORDER BY
        sfi.has_damage_guarantee ASC,   -- FALSE（補償なし）が先
        sfi.sku_id ASC                  -- 最小SKU IDが先
    ) AS rank_in_group
  FROM dup_duplicate_groups dg
  INNER JOIN dup_sku_full_info sfi
    ON dg.part_id_key = sfi.part_id_key
    AND sfi.is_corporate_item = FALSE  -- 法人小物管理用は重複グループから除外
    AND sfi.is_set_item = FALSE        -- セット品は重複グループから除外
    AND sfi.series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND sfi.series_name NOT LIKE '%【CLAS SET】%'
    AND sfi.series_name NOT LIKE '%エイトレント%'
    AND sfi.product_name NOT LIKE '%プラン%'
),

dup_excluded_skus AS (
  SELECT sku_id FROM dup_ranked_duplicates WHERE rank_in_group > 1
)

-- ============================================================
-- 9. 最終出力（連番方式：1からMAX(SKU_ID)まで全行出力）- 49カラム
-- ============================================================
SELECT
  -- 1. 画像
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    ELSE IFNULL(CONCAT(
      "https://clas.style/images/sku/",
      CAST(image.ref_id AS STRING),
      "/",
      LPAD(CAST(image.id AS STRING), 10, "0"),
      "_",
      TO_BASE64(image.hash),
      CASE image.file_type
        WHEN "Jpeg" THEN ".jpg"
        WHEN "Png" THEN ".png"
        WHEN "Gif" THEN ".gif"
        WHEN "Svg" THEN ".svg"
      END,
      "?type=Crop&width=1080&height=1080"
    ), "https://clas.style/images/noimage.jpg?type=Resize&width=540&height=540")
  END AS `画像リンク`,

  -- 2. 除外フラグ（除外対象なら「除外」、それ以外は空白）
  CASE
    WHEN spi.sku_id IS NULL THEN '除外'
    WHEN spi.is_deleted_sku = TRUE THEN '除外'
    WHEN spi.is_orphan_sku = TRUE THEN '除外'
    WHEN IFNULL(abi.suppliers, '') LIKE '%法人小物管理用%' THEN NULL  -- 空白（法人小物管理用は除外しない）
    WHEN spi.series_name LIKE '%【商品おまかせでおトク】%' THEN '除外'
    WHEN spi.series_name LIKE '%【CLAS SET】%' THEN '除外'
    WHEN spi.series_name LIKE '%エイトレント%' THEN '除外'
    WHEN spi.product_name LIKE '%セット%'
      AND spi.product_name NOT LIKE '%増連セット%'
      AND spi.product_name NOT LIKE '%片面開閉カバーセット%'
      AND spi.product_name NOT LIKE '%ハコ4色セット%'
      AND spi.product_name NOT LIKE '%ジョイントセット%'
      AND spi.product_name NOT LIKE '%インセットパネル%'
      THEN '除外'
    WHEN spi.product_name LIKE '%SET%' THEN '除外'
    WHEN spi.product_name LIKE '%プラン%' THEN '除外'
    WHEN dup_ex.sku_id IS NOT NULL THEN '除外'
    ELSE NULL  -- 空白
  END AS `除外フラグ`,

  -- 3. SKU_ID
  asi.seq_id AS `SKU_ID`,

  -- 4. 商品ID
  spi.product_id AS `商品ID`,

  -- 5. 商品IDリンク
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    ELSE CONCAT("https://clas.style/admin/product/", CAST(spi.product_id AS STRING), "#images")
  END AS `商品IDリンク`,

  -- 6. シリーズ名
  spi.series_name AS `シリーズ名`,

  -- 7. 商品名
  spi.product_name AS `商品名`,

  -- 8. 属性
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.value, '') END AS `属性`,

  -- 9. カテゴリ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(cm.category_ja, spi.category) END AS `カテゴリ`,

  -- 10. 対象顧客
  spi.customer AS `対象顧客`,

  -- 11. サプライヤー
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.suppliers, '') END AS `サプライヤー`,

  -- 12. 下代
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.cost, 0) END AS `下代`,

  -- 13. 上代
  spi.retail_price AS `上代`,

  -- 14. 構成パーツid（改行区切り）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.part_ids_str, '') END AS `構成パーツid`,

  -- 15. 構成パーツ名（改行区切り）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.part_names_str, '') END AS `構成パーツ名`,

  -- 16. 構成点数（改行区切り）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.quantities_str, '') END AS `構成点数`,

  -- 17. 2026年2月末_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q1_assembled_ids, '') END AS `2026年2月末_減損予定_在庫id_商品`,

  -- 18. 2026年5月末_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q2_assembled_ids, '') END AS `2026年5月末_減損予定_在庫id_商品`,

  -- 19. 2026年8月末_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q3_assembled_ids, '') END AS `2026年8月末_減損予定_在庫id_商品`,

  -- 20. 2026年11月末以降_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q4_assembled_ids, '') END AS `2026年11月末以降_減損予定_在庫id_商品`,

  -- 21. 減損済_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.impaired_assembled_ids, '') END AS `減損済_在庫id_商品`,

  -- 22. 2026年2月末_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q1_leftover_ids, '') END AS `2026年2月末_減損予定_在庫id_余りパーツ`,

  -- 23. 2026年5月末_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q2_leftover_ids, '') END AS `2026年5月末_減損予定_在庫id_余りパーツ`,

  -- 24. 2026年8月末_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q3_leftover_ids, '') END AS `2026年8月末_減損予定_在庫id_余りパーツ`,

  -- 25. 2026年11月末以降_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.q4_leftover_ids, '') END AS `2026年11月末以降_減損予定_在庫id_余りパーツ`,

  -- 26. 減損済_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.impaired_leftover_ids, '') END AS `減損済_在庫id_余りパーツ`,

  -- 27. 2026年2月末_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0))
  END AS `2026年2月末_減損予定_商品数`,

  -- 28. 2026年5月末_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q2_product_cumulative, 0) - IFNULL(sis.q1_product_cumulative, 0))
  END AS `2026年5月末_減損予定_商品数`,

  -- 29. 2026年8月末_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q3_product_cumulative, 0) - IFNULL(sis.q2_product_cumulative, 0))
  END AS `2026年8月末_減損予定_商品数`,

  -- 30. 2026年11月末以降_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q4_product_cumulative, 0) - IFNULL(sis.q3_product_cumulative, 0))
  END AS `2026年11月末以降_減損予定_商品数`,

  -- 31. 減損済_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.impaired_product_cnt, 0) END AS `減損済_商品数`,

  -- 32. 合計_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.total_assemblable, 0) END AS `合計_商品数`,

  -- 33. 2026年2月末_減損予定_余りパーツ数（Q1のみ：減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sls.leftover_q1_only_total, 0)
  END AS `2026年2月末_減損予定_余りパーツ数`,

  -- 34. 2026年5月末_減損予定_余りパーツ数（Q2のみ：Q1以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sls.leftover_q2_only_total, 0)
  END AS `2026年5月末_減損予定_余りパーツ数`,

  -- 35. 2026年8月末_減損予定_余りパーツ数（Q3のみ：Q2以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sls.leftover_q3_only_total, 0)
  END AS `2026年8月末_減損予定_余りパーツ数`,

  -- 36. 2026年11月末以降_減損予定_余りパーツ数（Q4のみ：Q3以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sls.leftover_q4_only_total, 0)
  END AS `2026年11月末以降_減損予定_余りパーツ数`,

  -- 37. 減損済_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_impaired_total, 0) END AS `減損済_余りパーツ数`,

  -- 38. 合計_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.total_leftover, 0) END AS `合計_余りパーツ数`,

  -- 39. 2026年2月末_減損予定_商品+余りパーツ（Q1のみ：減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0)) +
    IFNULL(sls.leftover_q1_only_total, 0)
  END AS `2026年2月末_減損予定_商品+余りパーツ`,

  -- 40. 2026年5月末_減損予定_商品+余りパーツ（Q2のみ：Q1以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q2_product_cumulative, 0) - IFNULL(sis.q1_product_cumulative, 0)) +
    IFNULL(sls.leftover_q2_only_total, 0)
  END AS `2026年5月末_減損予定_商品+余りパーツ`,

  -- 41. 2026年8月末_減損予定_商品+余りパーツ（Q3のみ：Q2以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q3_product_cumulative, 0) - IFNULL(sis.q2_product_cumulative, 0)) +
    IFNULL(sls.leftover_q3_only_total, 0)
  END AS `2026年8月末_減損予定_商品+余りパーツ`,

  -- 42. 2026年11月末以降_減損予定_商品+余りパーツ（Q4のみ：Q3以前・減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    GREATEST(0, IFNULL(sis.q4_product_cumulative, 0) - IFNULL(sis.q3_product_cumulative, 0)) +
    IFNULL(sls.leftover_q4_only_total, 0)
  END AS `2026年11月末以降_減損予定_商品+余りパーツ`,

  -- 43. 減損済_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.impaired_product_cnt, 0) + IFNULL(sls.leftover_impaired_total, 0)
  END AS `減損済_商品+余りパーツ`,

  -- 44. 合計_商品+余りパーツ累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.total_assemblable, 0) + IFNULL(sls.total_leftover, 0)
  END AS `合計_商品+余りパーツ累計`,

  -- 45. 2026年2月末_減損予定_商品+余りパーツ累計（Q1のみ：減損済み除外）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    -- Q1単体の商品+余りパーツ
    GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0)) +
    IFNULL(sls.leftover_q1_only_total, 0)
  END AS `2026年2月末_減損予定_商品+余りパーツ累計`,

  -- 46. 2026年5月末_減損予定_商品+余りパーツ累計（Q1+Q2）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    -- Q1 + Q2
    (GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0)) +
     IFNULL(sls.leftover_q1_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q2_product_cumulative, 0) - IFNULL(sis.q1_product_cumulative, 0)) +
     IFNULL(sls.leftover_q2_only_total, 0))
  END AS `2026年5月末_減損予定_商品+余りパーツ累計`,

  -- 47. 2026年8月末_減損予定_商品+余りパーツ累計（Q1+Q2+Q3）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    -- Q1 + Q2 + Q3
    (GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0)) +
     IFNULL(sls.leftover_q1_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q2_product_cumulative, 0) - IFNULL(sis.q1_product_cumulative, 0)) +
     IFNULL(sls.leftover_q2_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q3_product_cumulative, 0) - IFNULL(sis.q2_product_cumulative, 0)) +
     IFNULL(sls.leftover_q3_only_total, 0))
  END AS `2026年8月末_減損予定_商品+余りパーツ累計`,

  -- 48. 2026年11月末以降_減損予定_商品+余りパーツ累計（Q1+Q2+Q3+Q4）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    -- Q1 + Q2 + Q3 + Q4
    (GREATEST(0, IFNULL(sis.q1_product_cumulative, 0) - IFNULL(sis.impaired_product_cnt, 0)) +
     IFNULL(sls.leftover_q1_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q2_product_cumulative, 0) - IFNULL(sis.q1_product_cumulative, 0)) +
     IFNULL(sls.leftover_q2_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q3_product_cumulative, 0) - IFNULL(sis.q2_product_cumulative, 0)) +
     IFNULL(sls.leftover_q3_only_total, 0)) +
    (GREATEST(0, IFNULL(sis.q4_product_cumulative, 0) - IFNULL(sis.q3_product_cumulative, 0)) +
     IFNULL(sls.leftover_q4_only_total, 0))
  END AS `2026年11月末以降_減損予定_商品+余りパーツ累計`,

  -- 49. 滞留日数_平均_切上
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE CAST(CEIL(IFNULL(scra.avg_current_retention_days, 0)) AS INT64) END AS `滞留日数_平均_切上`

-- 連番テーブルを起点にLEFT JOIN
FROM all_sku_ids asi
LEFT OUTER JOIN sku_product_info spi ON spi.sku_id = asi.seq_id
LEFT OUTER JOIN attribute_based_info abi ON abi.sku_hash = spi.sku_hash
LEFT OUTER JOIN `lake.image` image ON image.type = 'Sku' AND image.ref_id = spi.product_id AND image.hint = spi.sku_hash AND image.deleted_at IS NULL
LEFT OUTER JOIN category_mapping cm ON cm.category_en = spi.category
LEFT OUTER JOIN sku_current_retention_avg scra ON scra.sku_hash = spi.sku_hash
LEFT OUTER JOIN dup_excluded_skus dup_ex ON dup_ex.sku_id = spi.sku_id
-- 減損情報
LEFT OUTER JOIN sku_impairment_summary sis ON sis.sku_hash = spi.sku_hash
LEFT OUTER JOIN sku_leftover_summary sls ON sls.sku_hash = spi.sku_hash
-- 構成パーツ情報
LEFT OUTER JOIN sku_part_info spi_info ON spi_info.sku_hash = spi.sku_hash
-- 在庫id詳細（統合CTE）
LEFT OUTER JOIN sku_stock_ids_unified stock_ids ON stock_ids.sku_hash = spi.sku_hash

-- SKU_ID昇順で並び替え
ORDER BY asi.seq_id ASC
;
