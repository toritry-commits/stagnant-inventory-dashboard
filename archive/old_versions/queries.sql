-- ============================================================
-- AI商品選定ツール用 完全統合クエリ（SKU起点版・連番方式）
-- ============================================================
-- 作成日: 2025-12-22
-- 更新日: 2025-12-22（連番方式に対応）
-- 目的: 商品選定ツールで使用する全データを1つのクエリで取得
--       ★SKU_ID連番方式：1からMAX(SKU_ID)まで全行出力（欠番は空行）
--
-- ★連番方式の特徴:
--   - 行番号 = SKU_ID + 1 で固定される
--   - AI分析結果が行ズレする心配がない
--   - 欠番（存在しないSKU_ID）は空行として出力
--   - 削除済みSKU、旧SKUも含めて全て出力
--
-- ★06版からの変更点:
--   1. SKU_ID連番を生成（1からMAX(SKU_ID)まで）
--   2. 存在しないSKU_IDは空行として出力
--   3. 孤立SKU（旧属性構成のSKU）も出力
--   4. 削除済みSKU（deleted_at IS NOT NULL）も出力
--
-- 統合内容:
--   1. 在庫マスター情報（シリーズ名・商品名分離、カテゴリ日本語化）
--   2. 在庫数（貸出可能、リカバリー）- 商品単位で各パーツの最小値
--   3. 戻り品情報（再利用可能日、利用可能数）- 商品単位で各パーツの最小値
--   4. 除外フラグ（is_excluded）: セット品・おまかせ・CLAS SET・エイトレント・プラン・重複SKU・旧SKU・削除済みSKU・欠番
--   5. 現在の平均滞留日数（倉庫在庫ベース：直近返却→現在）
--   6. 商品IDリンク（管理画面へのハイパーリンク）
--
-- 除外条件（is_excluded = TRUE）:
--   - ★欠番SKU（SKU_IDが存在しない）【最優先】
--   - ★削除済みSKU（deleted_at IS NOT NULL）【最優先】
--   - ★旧SKU（現在の属性構成と一致しないSKU）【最優先】
--   - シリーズ名に「【商品おまかせでおトク】」を含む
--   - シリーズ名に「【CLAS SET】」を含む
--   - シリーズ名に「エイトレント」を含む
--   - 商品名に「セット」を含む（※特定のセット名は除外対象外）
--   - 商品名に「プラン」を含む
--   - 重複SKU（同一part_id構成のSKUグループ内でrank 2以降）
--   ※ただし、サプライヤー名が「法人小物管理用」の場合は除外しない（削除済み・旧SKU・欠番以外）
--   ※除外対象外のセット名: 増連セット、片面開閉カバーセット、ハコ4色セット、ジョイントセット、木インセットパネル
--
-- 出力条件:
--   - ★1からMAX(SKU_ID)までの全連番を出力
--   - ★SKU_ID昇順で並び替え（連番なので自動的に昇順）
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
-- 1. 引当可能な在庫
-- ============================================================
stocks AS (
  SELECT s.id, s.status, s.part_id, s.part_version_id, s.supplier_id, w.business_area, s.location_id, s.arrival_at, s.inspected_at
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
    AND l.id NOT IN (5400, 5402,9389,9390,9521,9522,9523,9629,9702,9703,9951,10120,10355,10374,10415,10820, 10948,11022)
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
    AND (s._rank_ != 'S' OR s.supplier_id = 320 OR purchase.stock_id IS NOT NULL)
    AND external_sale.stock_id IS NULL
),

-- ============================================================
-- 2. AttributeValue単位の在庫集計
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
-- 3. カテゴリ名の日本語変換マッピング
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
-- 4. 戻り品情報（押さえ情報含む）をパーツ単位で集計→商品単位で最小値を取得
-- ============================================================
-- 4-1. 押さえ情報（次の案件に紐づいている在庫）
future_assignments AS (
  SELECT
    detail.stock_id,
    CAST(contract.start_date AS DATE) AS start_date,
    c.name AS client_name,
    p.name AS project_name
  FROM `lake.lent_to_b` ltb
  JOIN `lake.lent_to_b_detail` detail ON ltb.id = detail.lent_to_b_id
  LEFT JOIN `clas-analytics.lake.contract_destination` pd ON ltb.contract_destination_id = pd.id AND pd.deleted_at IS NULL
  LEFT JOIN `clas-analytics.lake.contract` contract ON pd.contract_id = contract.id AND contract.deleted_at IS NULL
  LEFT JOIN `clas-analytics.lake.proposition` p ON contract.proposition_id = p.id AND p.deleted_at IS NULL
  LEFT JOIN `clas-analytics.lake.client` c ON p.client_id = c.id
  WHERE ltb.start_at IS NULL
    AND ltb.status != 'Ended'
    AND ltb.deleted_at IS NULL
    AND detail.deleted_at IS NULL
),

-- 4-2. 戻り品情報（明日以降終了）- stock_idとpart_idを紐づけ
returns_stock AS (
  SELECT
    CAST(ltb.sku_id AS INT64) AS sku_id,
    sku.hash AS sku_hash,
    CAST(ltb.end_at AS DATE) AS return_date,
    CASE
      WHEN IFNULL(pd.area, 'Kanto') IN ('Kansai', 'Chugoku', 'Shikoku') THEN 'Kansai'
      WHEN IFNULL(pd.area, 'Kanto') IN ('Kyushu') THEN 'Kyushu'
      ELSE 'Kanto'
    END AS area,
    detail.stock_id,
    s.part_id,
    fa.stock_id AS reserved_stock_id  -- 押さえ済みの場合はNULLでない
  FROM `lake.lent_to_b` ltb
  JOIN `lake.lent_to_b_detail` detail ON ltb.id = detail.lent_to_b_id
  JOIN `clas-analytics.lake.contract_destination` pd ON ltb.contract_destination_id = pd.id
  JOIN `lake.sku` sku ON sku.id = CAST(ltb.sku_id AS INT64)
  JOIN `lake.stock` s ON s.id = detail.stock_id AND s.deleted_at IS NULL
  LEFT JOIN future_assignments fa ON detail.stock_id = fa.stock_id
  WHERE CAST(ltb.end_at AS DATE) >= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
    AND ltb.deleted_at IS NULL
    AND detail.deleted_at IS NULL
    AND pd.deleted_at IS NULL
    AND sku.deleted_at IS NULL
),

-- 4-3. SKU×パーツ×エリア×返却日ごとの戻り品集計（part_id単位でカウント）
returns_by_sku_part_area_date AS (
  SELECT
    sku_id,
    part_id,
    area,
    return_date,
    DATE_ADD(return_date, INTERVAL 9 DAY) AS available_from_date,
    COUNT(stock_id) AS quantity_total,
    COUNTIF(reserved_stock_id IS NOT NULL) AS quantity_reserved,
    COUNTIF(reserved_stock_id IS NULL) AS quantity_available
  FROM returns_stock
  GROUP BY sku_id, part_id, area, return_date
),

-- 4-3b. SKU×エリア×返却日単位で各パーツのLEAST（最小値）を取得
returns_by_sku_area_date AS (
  SELECT
    sku_id,
    area,
    return_date,
    available_from_date,
    MIN(quantity_total) AS quantity_total,
    MIN(quantity_reserved) AS quantity_reserved,
    MIN(quantity_available) AS quantity_available
  FROM returns_by_sku_part_area_date
  GROUP BY sku_id, area, return_date, available_from_date
),

-- 4-4. SKU×エリアごとで最も近い再利用可能日のデータのみ取得
returns_sku_earliest AS (
  SELECT
    sku_id,
    area,
    return_date,
    available_from_date,
    quantity_total,
    quantity_available
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY sku_id, area ORDER BY available_from_date ASC) AS rn
    FROM returns_by_sku_area_date
  )
  WHERE rn = 1
),

-- 4-5. SKU×エリア単位のピボット（最も近い再利用可能日のみ）
returns_sku_pivot AS (
  SELECT
    sku_id,
    -- 関東
    MAX(CASE WHEN area = 'Kanto' THEN available_from_date END) AS available_from_kanto,
    IFNULL(MAX(CASE WHEN area = 'Kanto' THEN quantity_available END), 0) AS returns_available_kanto,
    -- 関西
    MAX(CASE WHEN area = 'Kansai' THEN available_from_date END) AS available_from_kansai,
    IFNULL(MAX(CASE WHEN area = 'Kansai' THEN quantity_available END), 0) AS returns_available_kansai,
    -- 九州
    MAX(CASE WHEN area = 'Kyushu' THEN available_from_date END) AS available_from_kyushu,
    IFNULL(MAX(CASE WHEN area = 'Kyushu' THEN quantity_available END), 0) AS returns_available_kyushu,
    -- 戻り品合計（重複SKU除外で使用）
    IFNULL(MAX(CASE WHEN area = 'Kanto' THEN quantity_available END), 0)
      + IFNULL(MAX(CASE WHEN area = 'Kansai' THEN quantity_available END), 0)
      + IFNULL(MAX(CASE WHEN area = 'Kyushu' THEN quantity_available END), 0) AS returns_total
  FROM returns_sku_earliest
  GROUP BY sku_id
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

-- 5-5. 1周目（出荷済み）: 新品入庫 -> 初回法人出荷（EC出荷が挟まらない）
retention_round_1_shipped AS (
  SELECT
    sa.part_id,
    sa.stock_id,
    DATE_DIFF(CAST(b2b.first_start_at AS DATE), CAST(sa.arrival_at AS DATE), DAY) AS duration_days
  FROM stock_arrivals sa
  INNER JOIN first_b2b_ship b2b ON sa.stock_id = b2b.stock_id
  LEFT JOIN first_ec_ship ec ON sa.stock_id = ec.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND (ec.first_shipped_at IS NULL OR b2b.first_start_at <= ec.first_shipped_at)
),

-- 5-6. 2周目以降（出荷済み）: 返却 -> 次回法人出荷
retention_round_2_shipped AS (
  SELECT
    stock_id,
    DATE_DIFF(next_event_date, event_date, DAY) AS duration_days
  FROM (
    SELECT
      stock_id, event_date, event_type,
      LEAD(event_date, 1) OVER (PARTITION BY stock_id ORDER BY event_date) AS next_event_date,
      LEAD(event_type, 1) OVER (PARTITION BY stock_id ORDER BY event_date) AS next_event_type
    FROM all_lent_history
  )
  WHERE event_type IN ('EC_Return', 'B2B_Return')
    AND next_event_type = 'B2B_Ship'
),

-- 5-7. 現在倉庫在庫の滞留日数: 1周目（在庫中）- 新品入庫 -> 現在（EC出荷も法人出荷もまだ）
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

-- 5-8. 現在倉庫在庫の滞留日数: 2周目以降（在庫中）- 最終返却 -> 現在
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

-- 5-9. 現在倉庫在庫の滞留日数をpart_id単位で集計
all_current_retention AS (
  SELECT part_id, duration_days FROM retention_round_1_waiting WHERE duration_days IS NOT NULL AND duration_days >= 0
  UNION ALL
  SELECT s.part_id, r2.duration_days FROM retention_round_2_waiting r2 INNER JOIN `lake.stock` s ON r2.stock_id = s.id WHERE r2.duration_days IS NOT NULL AND r2.duration_days >= 0
),
part_current_retention_avg AS (
  SELECT part_id, AVG(duration_days) as avg_current_retention_days FROM all_current_retention GROUP BY part_id
),

-- 5-10. SKU→part_idマッピング（SKUテーブルのhashを使用）
sku_part_mapping AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    avp.part_id
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  INNER JOIN `lake.attribute` a ON a.product_id = pd.id AND a.deleted_at IS NULL
  INNER JOIN `lake.attribute_value` av ON av.attribute_id = a.id AND av.deleted_at IS NULL
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av.id AND avp.deleted_at IS NULL
  WHERE sku.deleted_at IS NULL
  GROUP BY sku.id, sku.hash, avp.part_id
),

-- 5-11. SKUごとの現在の平均滞留日数（倉庫在庫ベース）
sku_current_retention_avg AS (
  SELECT
    spm.sku_hash,
    ROUND(AVG(pcra.avg_current_retention_days), 1) AS avg_current_retention_days
  FROM sku_part_mapping spm
  LEFT JOIN part_current_retention_avg pcra ON spm.part_id = pcra.part_id
  GROUP BY spm.sku_hash
),

-- ============================================================
-- 6. 重複SKU除外判定
-- ============================================================
-- 6-1. SKUとattribute_valueの関係を取得（SKUテーブル起点）
dup_sku_attribute_mapping AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    pd.id AS product_id,
    pd.name AS product_name,
    se.name AS series_name,
    body.id AS body_av_id,
    leg.id AS leg_av_id,
    mr.id AS mattress_av_id,
    mrt.id AS mattress_topper_av_id,
    gr.id AS guarantee_av_id
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  INNER JOIN `lake.series` se ON se.id = pd.series_id AND se.deleted_at IS NULL
  LEFT JOIN (
    SELECT a.product_id, av.id
    FROM `lake.attribute` a
    INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.type = 'Body' AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  ) body ON body.product_id = pd.id
  LEFT JOIN (
    SELECT a.product_id, av.id
    FROM `lake.attribute` a
    INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.type = 'Leg' AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  ) leg ON leg.product_id = pd.id
  LEFT JOIN (
    SELECT a.product_id, av.id
    FROM `lake.attribute` a
    INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.type = 'Mattress' AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  ) mr ON mr.product_id = pd.id
  LEFT JOIN (
    SELECT a.product_id, av.id
    FROM `lake.attribute` a
    INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.type = 'MattressTopper' AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  ) mrt ON mrt.product_id = pd.id
  LEFT JOIN (
    SELECT a.product_id, av.id
    FROM `lake.attribute` a
    INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.type = 'Guarantee' AND a.deleted_at IS NULL AND av.deleted_at IS NULL
  ) gr ON gr.product_id = pd.id
  WHERE sku.deleted_at IS NULL
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

-- 6-8. 重複グループに属するSKUの詳細を出力（戻り品情報を結合）
-- ※セット品・CLAS SET・おまかせ等は既に別条件で除外されるため、重複判定から除外
dup_duplicate_skus AS (
  SELECT
    sfi.*,
    dg.sku_count AS duplicate_count,
    IFNULL(rsp.returns_total, 0) AS returns_total  -- 戻り品合計数
  FROM dup_duplicate_groups dg
  INNER JOIN dup_sku_full_info sfi
    ON dg.part_id_key = sfi.part_id_key
    AND sfi.is_corporate_item = FALSE  -- 法人小物管理用は重複グループから除外
    AND sfi.is_set_item = FALSE        -- セット品は重複グループから除外
    AND sfi.series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND sfi.series_name NOT LIKE '%【CLAS SET】%'
    AND sfi.series_name NOT LIKE '%エイトレント%'
    AND sfi.product_name NOT LIKE '%プラン%'
  LEFT JOIN returns_sku_pivot rsp ON rsp.sku_id = sfi.sku_id
),

-- 6-9. 各重複グループ内で、保持するSKUを決定
-- ルール（優先順位順）:
--   1. 戻り品数が多いSKUを優先（returns_total DESC）
--   2. 「補償: 汚損補償 付き」がないSKUを優先（has_damage_guarantee = FALSE）
--   3. 同じ条件の場合は、最小SKU IDを優先
dup_ranked_duplicates AS (
  SELECT
    sku_id,
    ROW_NUMBER() OVER (
      PARTITION BY part_id_key
      ORDER BY
        returns_total DESC,         -- 戻り品数が多い順
        has_damage_guarantee ASC,   -- FALSE（補償なし）が先
        sku_id ASC                  -- 最小SKU IDが先
    ) AS rank_in_group
  FROM dup_duplicate_skus
),

-- 6-10. 除外対象SKUのリスト（rank 2以降）
dup_excluded_skus AS (
  SELECT sku_id
  FROM dup_ranked_duplicates
  WHERE rank_in_group > 1
),

-- ============================================================
-- 7. ★SKU起点の商品情報取得（現在の属性構成から計算したhashとの照合）
-- ============================================================
-- 現在の属性構成から計算したhashのリスト（孤立SKU判定用）
current_attribute_hashes AS (
  SELECT DISTINCT
    TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.id, leg.id, mr.id, mrt.id, gr.id]) AS element ORDER BY element), ','))) AS sku_hash
  FROM `lake.series` se
  INNER JOIN `lake.product` pd ON pd.series_id = se.id AND se.deleted_at IS NULL AND pd.deleted_at IS NULL
  LEFT JOIN (SELECT a.product_id, av.id FROM `lake.attribute` a INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id WHERE a.type = 'Body' AND a.deleted_at IS NULL AND av.deleted_at IS NULL) body ON body.product_id = pd.id
  LEFT JOIN (SELECT a.product_id, av.id FROM `lake.attribute` a INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id WHERE a.type = 'Leg' AND a.deleted_at IS NULL AND av.deleted_at IS NULL) leg ON leg.product_id = pd.id
  LEFT JOIN (SELECT a.product_id, av.id FROM `lake.attribute` a INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id WHERE a.type = 'Mattress' AND a.deleted_at IS NULL AND av.deleted_at IS NULL) mr ON mr.product_id = pd.id
  LEFT JOIN (SELECT a.product_id, av.id FROM `lake.attribute` a INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id WHERE a.type = 'MattressTopper' AND a.deleted_at IS NULL AND av.deleted_at IS NULL) mrt ON mrt.product_id = pd.id
  LEFT JOIN (SELECT a.product_id, av.id FROM `lake.attribute` a INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id WHERE a.type = 'Guarantee' AND a.deleted_at IS NULL AND av.deleted_at IS NULL) gr ON gr.product_id = pd.id
),

-- ★SKUテーブルを起点とした商品情報（削除済みSKUも含む）
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
    pd.size_information AS size_text,
    pc.customer AS customer,
    -- 削除済みSKUかどうか
    CASE WHEN sku.deleted_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_deleted_sku,
    -- 現在の属性構成と一致するかどうか（旧SKU判定）
    CASE WHEN cah.sku_hash IS NOT NULL THEN FALSE ELSE TRUE END AS is_orphan_sku
  FROM `lake.sku` sku
  -- 削除済みSKUも含めるためLEFT JOINに変更（商品が削除されている場合も考慮）
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

-- ★現在の属性構成に基づく在庫情報（hashをキーに）
attribute_based_info AS (
  SELECT
    TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.id, leg.id, mr.id, mrt.id, gr.id]) AS element ORDER BY element), ','))) AS sku_hash,
    ARRAY_TO_STRING([body.value, leg.value, mr.value, mrt.value, gr.value], ' ') AS value,
    (SELECT STRING_AGG(DISTINCT suppliers) FROM UNNEST(ARRAY_CONCAT(IFNULL(body.suppliers, []), IFNULL(leg.suppliers, []), IFNULL(mr.suppliers, []), IFNULL(mrt.suppliers, []), IFNULL(gr.suppliers, []))) suppliers) AS suppliers,
    IFNULL(body.cost, 0) + IFNULL(leg.cost, 0) + IFNULL(mr.cost, 0) AS cost,
    IF(body.cnt_kanto IS NULL AND leg.cnt_kanto IS NULL AND mr.cnt_kanto IS NULL AND mrt.cnt_kanto IS NULL AND gr.cnt_kanto IS NULL, 0, LEAST(IFNULL(body.cnt_kanto, 9999), IFNULL(leg.cnt_kanto, 9999), IFNULL(mr.cnt_kanto, 9999), IFNULL(mrt.cnt_kanto, 9999))) AS available_kanto,
    IF(body.cnt_kansai IS NULL AND leg.cnt_kansai IS NULL AND mr.cnt_kansai IS NULL AND mrt.cnt_kansai IS NULL AND gr.cnt_kansai IS NULL, 0, LEAST(IFNULL(body.cnt_kansai, 9999), IFNULL(leg.cnt_kansai, 9999), IFNULL(mr.cnt_kansai, 9999), IFNULL(mrt.cnt_kansai, 9999))) AS available_kansai,
    IF(body.cnt_kyushu IS NULL AND leg.cnt_kyushu IS NULL AND mr.cnt_kyushu IS NULL AND mrt.cnt_kyushu IS NULL AND gr.cnt_kyushu IS NULL, 0, LEAST(IFNULL(body.cnt_kyushu, 9999), IFNULL(leg.cnt_kyushu, 9999), IFNULL(mr.cnt_kyushu, 9999), IFNULL(mrt.cnt_kyushu, 9999))) AS available_kyushu,
    IF(body.cnt_kanto_rec IS NULL AND leg.cnt_kanto_rec IS NULL AND mr.cnt_kanto_rec IS NULL AND mrt.cnt_kanto_rec IS NULL AND gr.cnt_kanto_rec IS NULL, 0, LEAST(IFNULL(body.cnt_kanto_rec, 9999), IFNULL(leg.cnt_kanto_rec, 9999), IFNULL(mr.cnt_kanto_rec, 9999), IFNULL(mrt.cnt_kanto_rec, 9999))) AS available_kanto_rec,
    IF(body.cnt_kansai_rec IS NULL AND leg.cnt_kansai_rec IS NULL AND mr.cnt_kansai_rec IS NULL AND mrt.cnt_kansai_rec IS NULL AND gr.cnt_kansai_rec IS NULL, 0, LEAST(IFNULL(body.cnt_kansai_rec, 9999), IFNULL(leg.cnt_kansai_rec, 9999), IFNULL(mr.cnt_kansai_rec, 9999), IFNULL(mrt.cnt_kansai_rec, 9999))) AS available_kansai_rec,
    IF(body.cnt_kyushu_rec IS NULL AND leg.cnt_kyushu_rec IS NULL AND mr.cnt_kyushu_rec IS NULL AND mrt.cnt_kyushu_rec IS NULL AND gr.cnt_kyushu_rec IS NULL, 0, LEAST(IFNULL(body.cnt_kyushu_rec, 9999), IFNULL(leg.cnt_kyushu_rec, 9999), IFNULL(mr.cnt_kyushu_rec, 9999), IFNULL(mrt.cnt_kyushu_rec, 9999))) AS available_kyushu_rec
  FROM `lake.series` se
  INNER JOIN `lake.product` pd ON pd.series_id = se.id AND se.deleted_at IS NULL AND pd.deleted_at IS NULL
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto, cnt_kanto_rec, cnt_kansai, cnt_kansai_rec, cnt_kyushu, cnt_kyushu_rec FROM avs WHERE `type` = 'Body') body ON body.product_id = pd.id
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto, cnt_kanto_rec, cnt_kansai, cnt_kansai_rec, cnt_kyushu, cnt_kyushu_rec FROM avs WHERE `type` = 'Leg') leg ON leg.product_id = pd.id
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto, cnt_kanto_rec, cnt_kansai, cnt_kansai_rec, cnt_kyushu, cnt_kyushu_rec FROM avs WHERE `type` = 'Mattress') mr ON mr.product_id = pd.id
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto, cnt_kanto_rec, cnt_kansai, cnt_kansai_rec, cnt_kyushu, cnt_kyushu_rec FROM avs WHERE `type` = 'MattressTopper') mrt ON mrt.product_id = pd.id
  LEFT OUTER JOIN (SELECT product_id, `type`, value, id, status, suppliers, cost, cnt_kanto, cnt_kanto_rec, cnt_kansai, cnt_kansai_rec, cnt_kyushu, cnt_kyushu_rec FROM avs WHERE `type` = 'Guarantee') gr ON gr.product_id = pd.id
)

-- ============================================================
-- 8. 最終出力（★連番方式：1からMAX(SKU_ID)まで全行出力）
-- ============================================================
SELECT
  -- ★連番のSKU_ID（欠番も含む）
  asi.seq_id AS `SKU_ID`,

  -- 基本情報（欠番の場合はNULL）
  spi.product_id AS `商品ID`,
  CASE
    WHEN spi.sku_id IS NULL THEN NULL  -- 欠番
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

  -- ★商品IDリンク（管理画面へのハイパーリンク）- 欠番の場合はNULL
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    ELSE CONCAT("https://clas.style/admin/product/", CAST(spi.product_id AS STRING), "#images")
  END AS `商品IDリンク`,

  -- 商品名情報（欠番の場合はNULL）
  spi.series_name AS `シリーズ名`,
  spi.product_name AS `商品名`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.value, '') END AS `属性`,

  -- カテゴリ・属性情報（欠番の場合はNULL）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(cm.category_ja, spi.category) END AS `カテゴリ`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(subcategory.names, '') END AS `サブカテゴリ`,
  spi.customer AS `対象顧客`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.suppliers, '') END AS `サプライヤー`,
  spi.brand_name AS `ブランド`,

  -- サイズ情報（商品仕様テキスト）- 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi.size_text, '') END AS `サイズ情報`,

  -- ★除外フラグ（TRUE=除外対象、FALSE=対象外）
  -- ★欠番、削除済みSKU、旧SKUは最優先で除外判定
  CASE
    WHEN spi.sku_id IS NULL THEN TRUE  -- ★欠番SKU（最優先）
    WHEN spi.is_deleted_sku = TRUE THEN TRUE  -- ★削除済みSKU
    WHEN spi.is_orphan_sku = TRUE THEN TRUE  -- ★旧SKU（現在の属性構成と不一致）
    WHEN IFNULL(abi.suppliers, '') LIKE '%法人小物管理用%' THEN FALSE
    WHEN spi.series_name LIKE '%【商品おまかせでおトク】%' THEN TRUE
    WHEN spi.series_name LIKE '%【CLAS SET】%' THEN TRUE
    WHEN spi.series_name LIKE '%エイトレント%' THEN TRUE
    -- セット判定（特定のセット名は除外対象外）
    WHEN spi.product_name LIKE '%セット%'
      AND spi.product_name NOT LIKE '%増連セット%'
      AND spi.product_name NOT LIKE '%片面開閉カバーセット%'
      AND spi.product_name NOT LIKE '%ハコ4色セット%'
      AND spi.product_name NOT LIKE '%ジョイントセット%'
      AND spi.product_name NOT LIKE '%インセットパネル%'
      THEN TRUE
    WHEN spi.product_name LIKE '%プラン%' THEN TRUE
    WHEN dup_ex.sku_id IS NOT NULL THEN TRUE  -- ★重複SKU除外
    ELSE FALSE
  END AS `除外フラグ`,

  -- 価格（欠番の場合はNULL）
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.cost, 0) END AS `下代`,
  spi.retail_price AS `上代`,

  -- ★滞留日数情報（切り上げて整数）- 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE CAST(CEIL(IFNULL(scra.avg_current_retention_days, 0)) AS INT64) END AS `滞留日数`,

  -- 在庫数（貸出可能）- 欠番・旧SKUは0またはNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kanto, 0) END AS `在庫数_関東_貸出可能`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kansai, 0) END AS `在庫数_関西_貸出可能`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kyushu, 0) END AS `在庫数_九州_貸出可能`,

  -- リカバリー品数 - 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kanto_rec, 0) - IFNULL(abi.available_kanto, 0) END AS `在庫数_関東_リカバリー`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kansai_rec, 0) - IFNULL(abi.available_kansai, 0) END AS `在庫数_関西_リカバリー`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.available_kyushu_rec, 0) - IFNULL(abi.available_kyushu, 0) END AS `在庫数_九州_リカバリー`,

  -- ★戻り品情報（関東）- 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL
       WHEN IFNULL(ret_sku.returns_available_kanto, 0) > 0 THEN ret_sku.available_from_kanto
       ELSE NULL END AS `戻り品_関東_利用可能日`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(ret_sku.returns_available_kanto, 0) END AS `戻り品数_関東`,

  -- ★戻り品情報（関西）- 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL
       WHEN IFNULL(ret_sku.returns_available_kansai, 0) > 0 THEN ret_sku.available_from_kansai
       ELSE NULL END AS `戻り品_関西_利用可能日`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(ret_sku.returns_available_kansai, 0) END AS `戻り品数_関西`,

  -- ★戻り品情報（九州）- 欠番の場合はNULL
  CASE WHEN spi.sku_id IS NULL THEN NULL
       WHEN IFNULL(ret_sku.returns_available_kyushu, 0) > 0 THEN ret_sku.available_from_kyushu
       ELSE NULL END AS `戻り品_九州_利用可能日`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(ret_sku.returns_available_kyushu, 0) END AS `戻り品数_九州`,

  -- ★デバッグ用フラグ
  CASE WHEN spi.sku_id IS NULL THEN TRUE ELSE FALSE END AS `欠番フラグ`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE spi.is_deleted_sku END AS `削除済みSKUフラグ`,
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE spi.is_orphan_sku END AS `旧SKUフラグ`

-- ★連番テーブルを起点にLEFT JOIN
FROM all_sku_ids asi
LEFT OUTER JOIN sku_product_info spi ON spi.sku_id = asi.seq_id
-- 在庫情報はhashで結合（旧SKU・欠番の場合はNULL）
LEFT OUTER JOIN attribute_based_info abi ON abi.sku_hash = spi.sku_hash
-- 画像情報
LEFT OUTER JOIN `lake.image` image ON image.type = 'Sku' AND image.ref_id = spi.product_id AND image.hint = spi.sku_hash AND image.deleted_at IS NULL
-- サブカテゴリ
LEFT OUTER JOIN (
  SELECT pt.product_id, STRING_AGG(tag.name) AS names
  FROM `lake.product_tag` pt
  INNER JOIN `lake.tag` tag ON pt.tag_id = tag.id
  WHERE pt.deleted_at IS NULL AND tag.deleted_at IS NULL AND tag.type = 'Subcategory'
  GROUP BY pt.product_id
) subcategory ON subcategory.product_id = spi.product_id
-- カテゴリ日本語化
LEFT OUTER JOIN category_mapping cm ON cm.category_en = spi.category
-- 滞留日数
LEFT OUTER JOIN sku_current_retention_avg scra ON scra.sku_hash = spi.sku_hash
-- 戻り品情報
LEFT OUTER JOIN returns_sku_pivot ret_sku ON ret_sku.sku_id = spi.sku_id
-- 重複SKU除外判定
LEFT OUTER JOIN dup_excluded_skus dup_ex ON dup_ex.sku_id = spi.sku_id
-- ★SKU_ID昇順で並び替え（連番なので自動的に1, 2, 3, ...）
ORDER BY asi.seq_id ASC
;
