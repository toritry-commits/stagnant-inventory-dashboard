-- ============================================================
-- 滞留在庫ダッシュボード用クエリ（v3: 在庫単位カテゴリ方式・動的期末）
-- ============================================================
-- 作成日: 2025-12-23
-- 更新日: 2026-01-19
-- 目的: 滞留在庫の可視化と減損対策のためのデータ取得
--
-- 変更履歴:
--   [2026-01-19] テーブル作成クエリに変更
--   - CREATE OR REPLACE TABLE を追加
--   - mart.stagnant_stock_report テーブルを置き換え
--
--   [2026-01-11] v3
--   - finance.fixed_asset_register への依存を排除
--     → 入庫日は lake.stock.arrival_at を直接使用
--   - 簿価・簿価累計カラムは在庫単位カテゴリで集計
--
--   [2025-12-26]
--   - 取得原価を簿価に変更（clas-analytics.mart.monthly_stock_valuationから取得）
--   - サプライヤー取得ロジック修正: deleted_atフィルタをサブクエリ内に移動
--   - business_area IS NULLの在庫（法人直送倉庫）を全エリアのカウントから除外
--   - 簿価平均単価カラム追加（商品のみの簿価 / 商品数）
--   - 在庫ID表示形式を[パーツID:倉庫:点数]に変更
--   - ロケーション除外条件を削除（全ロケーションを対象に変更）
--
--   [2025-12-23] 初版作成
--   - 減損カテゴリ判定を「累計方式」から「排他カテゴリ方式」に変更
--   - 組み上げ優先順位: term1 > term2 > term3 > term4_after > impaired
--   - 商品の減損時期: 減損済みを除いて最も早い減損カテゴリで決定
--   - 期末日を動的計算（実行日から次の2月末/5月末/8月末/11月末を自動算出）
--
-- 主な機能:
--   1. SKU単位での滞留在庫情報（連番方式：1からMAX(SKU_ID)まで全行出力）
--   2. 滞留日数（平均、切り上げ）
--   3. 減損予定点数（期末1/2/3/4以降）- 動的に算出
--   4. 商品数（組み上げ分）/余りパーツ数の分離表示
--   5. 構成パーツ情報（パーツid、パーツ名、構成点数）
--   6. 在庫id詳細（減損対象の在庫idをパーツごとに表示）
--   7. 合計カラム（商品数、余りパーツ数、商品+余りパーツ）
--   8. 期末日カラム（期末1_日付, 期末2_日付, 期末3_日付）で実際の期末日を表示
--   9. 簿価（期末1〜4以降の減損予定分、累計）- 減損済みは除外
--   10. 簿価平均単価（商品のみ）
--
-- 減損判定基準:
--   - 365日以上滞留で減損対象
--   - カテゴリ判定: 滞留開始日+365日がどの期末以前か
--
-- エリア別計算:
--   - 減損数は関東/関西/九州ごとに計算してから合計
--   - 法人直送倉庫（business_area IS NULL）はカウント対象外
--
-- 出力条件:
--   - 1からMAX(SKU_ID)までの全連番を出力
--   - SKU_ID昇順で並び替え
--
-- 依存テーブル:
--   - lake.* (大部分)
--   - clas-analytics.mart.monthly_stock_valuation（簿価データ）
-- ============================================================

CREATE OR REPLACE TABLE `clas-analytics.mart.stagnant_stock_report` AS

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
-- 1. 期末日付の動的計算（実行日から次の2月末/5月末/8月末/11月末を算出）
-- ============================================================
-- 期末月の候補: 2, 5, 8, 11（3ヶ月周期）
quarter_ends AS (
  SELECT
    CURRENT_DATE() AS today,
    -- 今日の月から次の期末月を計算
    -- 期末月候補: 2, 5, 8, 11
    (SELECT MIN(candidate_date)
     FROM UNNEST([
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 11, 30)
     ]) AS candidate_date
     WHERE candidate_date > CURRENT_DATE()
    ) AS term1_end,
    (SELECT ARRAY_AGG(candidate_date ORDER BY candidate_date)[OFFSET(1)]
     FROM UNNEST([
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 11, 30)
     ]) AS candidate_date
     WHERE candidate_date > CURRENT_DATE()
    ) AS term2_end,
    (SELECT ARRAY_AGG(candidate_date ORDER BY candidate_date)[OFFSET(2)]
     FROM UNNEST([
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 11, 30)
     ]) AS candidate_date
     WHERE candidate_date > CURRENT_DATE()
    ) AS term3_end,
    (SELECT ARRAY_AGG(candidate_date ORDER BY candidate_date)[OFFSET(3)]
     FROM UNNEST([
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 11, 30),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 5, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 8, 31),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 11, 30)
     ]) AS candidate_date
     WHERE candidate_date > CURRENT_DATE()
    ) AS term4_end
),

-- ============================================================
-- 2. 引当可能な在庫（queries.sql準拠）
-- ============================================================
-- 滞留在庫の対象となる在庫を抽出するCTE
-- 以下の条件で除外される在庫は滞留在庫としてカウントしない
stocks AS (
  SELECT
    s.id,
    s.status,
    s.part_id,
    s.part_version_id,
    s.supplier_id,
    w.business_area,
    -- 倉庫名の短縮表記（ダッシュボード表示用）
    CASE w.name
      WHEN '法人直送' THEN '船橋'
      WHEN '門真倉庫' THEN '門真'
      WHEN '東葛西ロジスティクスセンター' THEN '東葛西'
      WHEN '株式会社MAKE VALUE　川崎倉庫' THEN '川崎'
      WHEN '船橋' THEN '船橋'
      WHEN 'キタザワ九州倉庫' THEN '九州'
      ELSE w.name
    END AS warehouse_name,
    s.location_id,
    s.arrival_at,
    s.inspected_at,
    s.impairment_date,
    s.cost
  FROM `lake.stock` s
  INNER JOIN `lake.part` p ON p.id = s.part_id AND p.deleted_at IS NULL
  INNER JOIN `lake.part_version` pv ON pv.id = s.part_version_id AND pv.deleted_at IS NULL
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
  -- 除外条件1: toC引当準備中/調整中の在庫
  LEFT OUTER JOIN (
    SELECT ld.stock_id
    FROM `lake.lent` l INNER JOIN `lake.lent_detail` ld ON l.id = ld.lent_id
    WHERE l.status IN ('Preparing', 'Adjusting') AND l.deleted_at IS NULL AND ld.deleted_at IS NULL
    GROUP BY ld.stock_id
  ) to_c ON to_c.stock_id = s.id
  -- 除外条件2: toB引当準備中の在庫
  LEFT OUTER JOIN (
    SELECT d.stock_id
    FROM `lake.lent_to_b` b INNER JOIN `lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing' AND b.deleted_at IS NULL AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  -- 除外条件3: 特定メモタグ（id=64）が付いている在庫
  LEFT OUTER JOIN (
    SELECT sm.stock_id
    FROM `lake.staff_memo` sm
    INNER JOIN `lake.staff_memo_tag` smt ON sm.id = smt.staff_memo_id AND sm.stock_id IS NOT NULL
    INNER JOIN `lake.memo_tag` mt ON mt.id = smt.memo_tag_id AND mt.id = 64
    WHERE sm.deleted_at IS NULL AND smt.deleted_at IS NULL AND mt.deleted_at IS NULL
    GROUP BY sm.stock_id
  ) tag ON tag.stock_id = s.id
  -- 除外条件4: B向け発注済み在庫
  LEFT OUTER JOIN (
    SELECT pds.stock_id
    FROM `lake.purchasing_detail_stock` pds
    INNER JOIN `lake.purchasing_detail` pd ON pds.purchasing_detail_id = pd.id
    INNER JOIN `lake.purchasing` p ON p.id = pd.purchasing_id
    WHERE pds.deleted_at IS NULL AND pd.deleted_at IS NULL AND p.deleted_at IS NULL
      AND p.status = 'Done' AND p.orderer_email = 'info+b-order@clas.style'
    GROUP BY pds.stock_id
  ) purchase ON purchase.stock_id = s.id
  -- 除外条件5: 外部販売中の在庫
  LEFT OUTER JOIN (
    SELECT xss.stock_id
    FROM `lake.external_sale_stock` xss
    INNER JOIN `lake.external_sale_product` xsp ON xsp.id = xss.external_sale_product_id
    WHERE xss.deleted_at IS NULL AND xsp.deleted_at IS NULL AND xsp.status != 'Deny'
    GROUP BY xss.stock_id
  ) external_sale ON external_sale.stock_id = s.id
  WHERE s.deleted_at IS NULL
    -- ステータス条件: Ready/Waiting、またはRecovery（特定パーツを除く）
    AND (s.status IN ('Ready', 'Waiting') OR (s.status = 'Recovery' AND s.part_id NOT IN (7108,7109,7110,7438,7439,7440,7570,7571,7572,7573)))
    AND tag.stock_id IS NULL      -- メモタグ除外
    AND to_c.stock_id IS NULL     -- toC引当除外
    AND to_b.stock_id IS NULL     -- toB引当除外
    AND w.available_for_business = TRUE  -- 営業利用可能な倉庫のみ
    AND s._rank_ NOT IN ('R', 'L')       -- ランクR/Lは除外
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)  -- 検品不要は除外
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
    AND external_sale.stock_id IS NULL  -- 外部販売除外
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
    -- サプライヤー取得: 削除されていないバージョンの中で最大バージョンのサプライヤーを取得
    -- ※サブクエリ内でdeleted_atをフィルタすることで、最大バージョンが論理削除されている場合でも
    --   正しいサプライヤーを取得可能（外側でフィルタすると取得できないケースがある）
    INNER JOIN (
      SELECT pv.part_id, sup.name
      FROM `lake.part_version` pv
      INNER JOIN (
        SELECT pv2.part_id, MAX(pv2.version) AS max_ver
        FROM `lake.part_version` pv2
        WHERE pv2.deleted_at IS NULL  -- ここでフィルタすることが重要
        GROUP BY pv2.part_id
      ) pv2 ON pv.version = pv2.max_ver AND pv.part_id = pv2.part_id
      INNER JOIN `lake.supplier` sup ON sup.id = pv.supplier_id
      WHERE sup.deleted_at IS NULL
    ) supplier ON avp.part_id = supplier.part_id
    -- エリア別在庫数カウント（business_area IS NULLの法人直送倉庫はカウント対象外）
    -- Ready/Waiting在庫
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status IN ('Ready','Waiting') AND ss.business_area = 'Kanto' GROUP BY ss.part_id) available_stock_kanto ON available_stock_kanto.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status IN ('Ready','Waiting') AND ss.business_area = 'Kansai' GROUP BY ss.part_id) available_stock_kansai ON available_stock_kansai.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status IN ('Ready','Waiting') AND ss.business_area = 'Kyushu' GROUP BY ss.part_id) available_stock_kyushu ON available_stock_kyushu.part_id = avp.part_id
    -- Recovery在庫（回収中）
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND ss.business_area = 'Kanto' GROUP BY ss.part_id) available_recovery_stock_kanto ON available_recovery_stock_kanto.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND ss.business_area = 'Kansai' GROUP BY ss.part_id) available_recovery_stock_kansai ON available_recovery_stock_kansai.part_id = avp.part_id
    LEFT OUTER JOIN (SELECT COUNT(ss.id) AS cnt, ss.part_id FROM stocks ss WHERE ss.status = 'Recovery' AND ss.business_area = 'Kyushu' GROUP BY ss.part_id) available_recovery_stock_kyushu ON available_recovery_stock_kyushu.part_id = avp.part_id
    WHERE avp.deleted_at IS NULL
    GROUP BY avp.id
  ) avp ON avp.av_id = av.id
  GROUP BY av.id
),

-- ============================================================
-- 4. 商品ステータス名の日本語変換マッピング
-- ============================================================
product_status_mapping AS (
  SELECT status_en, status_ja FROM UNNEST([
    STRUCT('OnSale' AS status_en, '販売中' AS status_ja),
    STRUCT('Soldout', '売り切れ'),
    STRUCT('Stopped', '販売停止')
  ])
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
-- 5-1. 全在庫の「入庫日」を特定（lake.stockから直接取得）
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    s.arrival_at
  FROM `lake.stock` s
  WHERE s.deleted_at IS NULL
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
stock_retention_start AS (
  -- 1周目: 新品入庫→現在在庫中（一度も出荷されていない）
  SELECT
    sa.stock_id,
    s.part_id,
    s.business_area,
    s.warehouse_name,
    s.impairment_date,
    s.cost,
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
    s.warehouse_name,
    s.impairment_date,
    s.cost,
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
-- 6-4. 在庫ごとの排他的カテゴリ判定（v2: 新ロジック）
-- ============================================================
-- 各在庫に1つのカテゴリを割り当て
-- カテゴリ: impaired, term1, term2, term3, term4_after
stock_with_category AS (
  SELECT
    srs.stock_id,
    srs.part_id,
    srs.business_area,
    srs.warehouse_name,
    srs.retention_start_date,
    srs.cost,
    -- 排他的カテゴリ判定（動的期末日を使用）
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 'impaired'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 'impaired'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 'term1'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 'term2'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 'term3'
      ELSE 'term4_after'
    END AS category,
    -- 商品判定用: カテゴリの優先順位（減損済みは99で除外用）
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 1
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 2
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 3
      ELSE 4
    END AS category_order
  FROM stock_retention_start srs
  CROSS JOIN quarter_ends qe
),

-- ============================================================
-- 6-5. 全エリアリスト（在庫が存在するエリアのみ）
-- ============================================================
all_areas AS (
  SELECT DISTINCT business_area FROM stock_with_category
),

-- ============================================================
-- 6-5-2. SKU×エリア×パーツの全組み合わせを生成
-- ============================================================
sku_area_part_full AS (
  SELECT spd.sku_hash, spd.part_id, spd.part_type, spd.quantity, areas.business_area
  FROM sku_part_detail spd
  CROSS JOIN all_areas areas
),

-- ============================================================
-- 6-5-3. SKU・エリア・パーツ別の在庫数（カテゴリ別）
-- ============================================================
-- CROSS JOINでエリア×パーツの全組み合わせを生成してからLEFT JOINで在庫をカウント
-- これにより、片方のパーツが0個でも正しく0としてカウントされる
sku_area_part_stock AS (
  SELECT
    sapf.sku_hash,
    sapf.part_id,
    sapf.part_type,
    sapf.quantity,
    sapf.business_area,
    COUNT(swc.stock_id) AS stock_cnt,
    -- 各カテゴリの在庫数
    COUNT(CASE WHEN swc.category = 'impaired' THEN swc.stock_id END) AS impaired_cnt,
    COUNT(CASE WHEN swc.category = 'term1' THEN swc.stock_id END) AS term1_cnt,
    COUNT(CASE WHEN swc.category = 'term2' THEN swc.stock_id END) AS term2_cnt,
    COUNT(CASE WHEN swc.category = 'term3' THEN swc.stock_id END) AS term3_cnt,
    COUNT(CASE WHEN swc.category = 'term4_after' THEN swc.stock_id END) AS term4_cnt
  FROM sku_area_part_full sapf
  LEFT JOIN stock_with_category swc ON sapf.part_id = swc.part_id AND sapf.business_area = swc.business_area
  GROUP BY sapf.sku_hash, sapf.part_id, sapf.part_type, sapf.quantity, sapf.business_area
),

-- ============================================================
-- 6-6. SKU・エリア別の組み上げ可能数を計算
-- ============================================================
sku_area_assemblable AS (
  SELECT
    sku_hash,
    business_area,
    -- 組み上げ可能数 = 各パーツの在庫数÷quantityの最小値
    MIN(CAST(stock_cnt / quantity AS INT64)) AS assemblable_cnt
  FROM sku_area_part_stock
  GROUP BY sku_hash, business_area
),

-- ============================================================
-- 6-7. 組み上げ優先順で在庫に順番を付与
-- ============================================================
-- 優先度: term1 > term2 > term3 > term4_after > impaired
stock_with_order AS (
  SELECT
    spd.sku_hash,
    swc.stock_id,
    swc.part_id,
    spd.quantity,
    swc.business_area,
    swc.warehouse_name,
    swc.category,
    swc.category_order,
    swc.cost,
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, swc.part_id, swc.business_area
      ORDER BY
        CASE swc.category
          WHEN 'term1' THEN 1
          WHEN 'term2' THEN 2
          WHEN 'term3' THEN 3
          WHEN 'term4_after' THEN 4
          WHEN 'impaired' THEN 5
        END,
        swc.stock_id
    ) AS stock_order
  FROM sku_part_detail spd
  INNER JOIN stock_with_category swc ON spd.part_id = swc.part_id
),

-- ============================================================
-- 6-8. 組み上げ用在庫と余り在庫の分離
-- ============================================================
stock_assembly_classification AS (
  SELECT
    swo.sku_hash,
    swo.stock_id,
    swo.part_id,
    swo.quantity,
    swo.business_area,
    swo.warehouse_name,
    swo.category,
    swo.category_order,
    swo.cost,
    swo.stock_order,
    saa.assemblable_cnt,
    -- 組み上げ用かどうか
    CASE WHEN swo.stock_order <= saa.assemblable_cnt * swo.quantity THEN TRUE ELSE FALSE END AS is_assembled,
    -- 商品番号（組み上げ用のみ）
    CASE WHEN swo.stock_order <= saa.assemblable_cnt * swo.quantity
      THEN CAST(CEIL(swo.stock_order / swo.quantity) AS INT64)
      ELSE NULL
    END AS product_no
  FROM stock_with_order swo
  LEFT JOIN sku_area_assemblable saa
    ON swo.sku_hash = saa.sku_hash AND swo.business_area = saa.business_area
),

-- ============================================================
-- 6-9. 商品ごとの減損時期判定（減損済み除いて最小カテゴリ）
-- ============================================================
product_category AS (
  SELECT
    sku_hash,
    business_area,
    product_no,
    -- 減損済みを除いて最も早い減損カテゴリを取得
    MIN(CASE WHEN category != 'impaired' THEN category_order END) AS earliest_order
  FROM stock_assembly_classification
  WHERE is_assembled = TRUE AND product_no IS NOT NULL
  GROUP BY sku_hash, business_area, product_no
),

-- 商品に減損カテゴリを付与
product_with_category AS (
  SELECT
    pc.sku_hash,
    pc.business_area,
    pc.product_no,
    CASE
      WHEN pc.earliest_order IS NULL THEN 'all_impaired'
      WHEN pc.earliest_order = 1 THEN 'term1'
      WHEN pc.earliest_order = 2 THEN 'term2'
      WHEN pc.earliest_order = 3 THEN 'term3'
      WHEN pc.earliest_order = 4 THEN 'term4_after'
    END AS product_category
  FROM product_category pc
),

-- ============================================================
-- 6-10. SKU別の商品数集計（カテゴリ別）
-- ============================================================
sku_impairment_summary AS (
  SELECT
    sku_hash,
    -- 組み上げ可能数（商品数）= product_with_categoryの行数
    COUNT(*) AS total_assemblable,
    -- 減損済み（商品数）- 全パーツが減損済みの場合
    SUM(CASE WHEN product_category = 'all_impaired' THEN 1 ELSE 0 END) AS impaired_product_cnt,
    -- 期末1減損予定（商品数）
    SUM(CASE WHEN product_category = 'term1' THEN 1 ELSE 0 END) AS term1_product_cnt,
    -- 期末2減損予定（商品数）
    SUM(CASE WHEN product_category = 'term2' THEN 1 ELSE 0 END) AS term2_product_cnt,
    -- 期末3減損予定（商品数）
    SUM(CASE WHEN product_category = 'term3' THEN 1 ELSE 0 END) AS term3_product_cnt,
    -- 期末4以降減損予定（商品数）
    SUM(CASE WHEN product_category = 'term4_after' THEN 1 ELSE 0 END) AS term4_product_cnt
  FROM product_with_category
  GROUP BY sku_hash
),

-- ============================================================
-- 6-11. 余りパーツのカテゴリ別集計
-- ============================================================
sku_area_leftover AS (
  SELECT
    sac.sku_hash,
    sac.part_id,
    sac.business_area,
    sac.category,
    COUNT(*) AS leftover_cnt
  FROM stock_assembly_classification sac
  WHERE sac.is_assembled = FALSE
  GROUP BY sac.sku_hash, sac.part_id, sac.business_area, sac.category
),

-- ============================================================
-- 6-12. SKU別の余りパーツ合計
-- ============================================================
sku_leftover_summary AS (
  SELECT
    sku_hash,
    -- 余りパーツ数の合計
    SUM(leftover_cnt) AS total_leftover,
    -- 減損済み余りパーツ
    SUM(CASE WHEN category = 'impaired' THEN leftover_cnt ELSE 0 END) AS leftover_impaired_total,
    -- 期末1減損予定余りパーツ
    SUM(CASE WHEN category = 'term1' THEN leftover_cnt ELSE 0 END) AS leftover_term1_total,
    -- 期末2減損予定余りパーツ
    SUM(CASE WHEN category = 'term2' THEN leftover_cnt ELSE 0 END) AS leftover_term2_total,
    -- 期末3減損予定余りパーツ
    SUM(CASE WHEN category = 'term3' THEN leftover_cnt ELSE 0 END) AS leftover_term3_total,
    -- 期末4以降減損予定余りパーツ
    SUM(CASE WHEN category = 'term4_after' THEN leftover_cnt ELSE 0 END) AS leftover_term4_total
  FROM sku_area_leftover
  GROUP BY sku_hash
),

-- ============================================================
-- 6-12-2. 期末別の簿価データ取得（簿価テーブルから）
-- ============================================================
-- 簿価テーブルから各期末日時点のbook_value_closingを取得
stock_book_value AS (
  SELECT
    msv.stock_id,
    msv.period_end,
    msv.book_value_closing
  FROM `clas-analytics.mart.monthly_stock_valuation` msv
  INNER JOIN quarter_ends qe
    ON msv.period_end IN (qe.term1_end, qe.term2_end, qe.term3_end, qe.term4_end)
),

-- ============================================================
-- 6-12-3. SKU別の簿価集計（減損予定分のみ、減損済み除外）
-- ============================================================
-- 商品の簿価: 商品カテゴリ(term1〜term4_after)ごとに集計
-- 余りパーツの簿価: 在庫カテゴリ(term1〜term4_after)ごとに集計
-- 各termの期末日時点での簿価（book_value_closing）を使用
sku_book_value_summary AS (
  SELECT
    sac.sku_hash,
    -- 期末1減損予定の簿価（商品＋余りパーツ）- 在庫個別カテゴリで集計
    SUM(CASE
      WHEN sac.category = 'term1'
      THEN IFNULL(bv1.book_value_closing, 0)
      ELSE 0
    END) AS term1_book_value,
    -- 期末2減損予定の簿価（商品＋余りパーツ）- 在庫個別カテゴリで集計
    SUM(CASE
      WHEN sac.category = 'term2'
      THEN IFNULL(bv2.book_value_closing, 0)
      ELSE 0
    END) AS term2_book_value,
    -- 期末3減損予定の簿価（商品＋余りパーツ）- 在庫個別カテゴリで集計
    SUM(CASE
      WHEN sac.category = 'term3'
      THEN IFNULL(bv3.book_value_closing, 0)
      ELSE 0
    END) AS term3_book_value,
    -- 期末4以降減損予定の簿価（商品＋余りパーツ）- 在庫個別カテゴリで集計
    SUM(CASE
      WHEN sac.category = 'term4_after'
      THEN IFNULL(bv4.book_value_closing, 0)
      ELSE 0
    END) AS term4_book_value,
    -- 期末1減損予定の簿価（商品のみ）- 平均単価計算用
    SUM(CASE
      WHEN sac.is_assembled AND pwc.product_category = 'term1'
      THEN IFNULL(bv1.book_value_closing, 0)
      ELSE 0
    END) AS term1_product_book_value,
    -- 期末2減損予定の簿価（商品のみ）- 平均単価計算用
    SUM(CASE
      WHEN sac.is_assembled AND pwc.product_category = 'term2'
      THEN IFNULL(bv2.book_value_closing, 0)
      ELSE 0
    END) AS term2_product_book_value,
    -- 期末3減損予定の簿価（商品のみ）- 平均単価計算用
    SUM(CASE
      WHEN sac.is_assembled AND pwc.product_category = 'term3'
      THEN IFNULL(bv3.book_value_closing, 0)
      ELSE 0
    END) AS term3_product_book_value,
    -- 期末4以降減損予定の簿価（商品のみ）- 平均単価計算用
    SUM(CASE
      WHEN sac.is_assembled AND pwc.product_category = 'term4_after'
      THEN IFNULL(bv4.book_value_closing, 0)
      ELSE 0
    END) AS term4_product_book_value
  FROM stock_assembly_classification sac
  LEFT JOIN product_with_category pwc
    ON sac.sku_hash = pwc.sku_hash
    AND sac.business_area = pwc.business_area
    AND sac.product_no = pwc.product_no
  CROSS JOIN quarter_ends qe
  LEFT JOIN stock_book_value bv1
    ON sac.stock_id = bv1.stock_id AND bv1.period_end = qe.term1_end
  LEFT JOIN stock_book_value bv2
    ON sac.stock_id = bv2.stock_id AND bv2.period_end = qe.term2_end
  LEFT JOIN stock_book_value bv3
    ON sac.stock_id = bv3.stock_id AND bv3.period_end = qe.term3_end
  LEFT JOIN stock_book_value bv4
    ON sac.stock_id = bv4.stock_id AND bv4.period_end = qe.term4_end
  GROUP BY sac.sku_hash
),

-- ============================================================
-- 6-13. 在庫ID表示用（組み上げ・余り別、カテゴリ別）- [パーツid:倉庫名:〇点]形式
-- ============================================================
-- まず、パーツ×倉庫別に在庫IDを集約
-- ●マーク: 該当termで実際に減損対象となる在庫（在庫のcategoryが該当termと一致）
sku_stock_ids_by_warehouse AS (
  SELECT
    sac.sku_hash,
    sac.part_id,
    sac.warehouse_name,
    -- 商品用在庫ID（商品カテゴリで分類）- ●は実際に減損対象となる在庫
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'term1'
      THEN CONCAT(CASE WHEN sac.category = 'term1' THEN '●' ELSE '' END, CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term1_assembled_ids,
    COUNT(CASE WHEN sac.is_assembled AND pwc.product_category = 'term1' THEN 1 END) AS term1_assembled_cnt,
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'term2'
      THEN CONCAT(CASE WHEN sac.category = 'term2' THEN '●' ELSE '' END, CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term2_assembled_ids,
    COUNT(CASE WHEN sac.is_assembled AND pwc.product_category = 'term2' THEN 1 END) AS term2_assembled_cnt,
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'term3'
      THEN CONCAT(CASE WHEN sac.category = 'term3' THEN '●' ELSE '' END, CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term3_assembled_ids,
    COUNT(CASE WHEN sac.is_assembled AND pwc.product_category = 'term3' THEN 1 END) AS term3_assembled_cnt,
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'term4_after'
      THEN CONCAT(CASE WHEN sac.category = 'term4_after' THEN '●' ELSE '' END, CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term4_assembled_ids,
    COUNT(CASE WHEN sac.is_assembled AND pwc.product_category = 'term4_after' THEN 1 END) AS term4_assembled_cnt,
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'all_impaired'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS impaired_assembled_ids,
    COUNT(CASE WHEN sac.is_assembled AND pwc.product_category = 'all_impaired' THEN 1 END) AS impaired_assembled_cnt,
    -- 余りパーツ用在庫ID（在庫のカテゴリで分類）- 余りパーツは該当termに属する=減損対象なので全て●
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'term1'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term1_leftover_ids,
    COUNT(CASE WHEN NOT sac.is_assembled AND sac.category = 'term1' THEN 1 END) AS term1_leftover_cnt,
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'term2'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term2_leftover_ids,
    COUNT(CASE WHEN NOT sac.is_assembled AND sac.category = 'term2' THEN 1 END) AS term2_leftover_cnt,
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'term3'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term3_leftover_ids,
    COUNT(CASE WHEN NOT sac.is_assembled AND sac.category = 'term3' THEN 1 END) AS term3_leftover_cnt,
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'term4_after'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS term4_leftover_ids,
    COUNT(CASE WHEN NOT sac.is_assembled AND sac.category = 'term4_after' THEN 1 END) AS term4_leftover_cnt,
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'impaired'
      THEN CONCAT('●', CAST(sac.stock_id AS STRING))
      END, ', ' ORDER BY sac.stock_id) AS impaired_leftover_ids,
    COUNT(CASE WHEN NOT sac.is_assembled AND sac.category = 'impaired' THEN 1 END) AS impaired_leftover_cnt
  FROM stock_assembly_classification sac
  LEFT JOIN product_with_category pwc
    ON sac.sku_hash = pwc.sku_hash
    AND sac.business_area = pwc.business_area
    AND sac.product_no = pwc.product_no
  GROUP BY sac.sku_hash, sac.part_id, sac.warehouse_name
),

-- [パーツid:倉庫名:〇点]形式でSKU別に集約
sku_stock_ids_unified AS (
  SELECT
    sku_hash,
    -- 組み上げ対象の在庫id（商品カテゴリ別）- [パーツid:倉庫名:〇点]形式
    STRING_AGG(CASE WHEN term1_assembled_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term1_assembled_cnt AS STRING), '点]', term1_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term1_assembled_ids,
    STRING_AGG(CASE WHEN term2_assembled_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term2_assembled_cnt AS STRING), '点]', term2_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term2_assembled_ids,
    STRING_AGG(CASE WHEN term3_assembled_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term3_assembled_cnt AS STRING), '点]', term3_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term3_assembled_ids,
    STRING_AGG(CASE WHEN term4_assembled_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term4_assembled_cnt AS STRING), '点]', term4_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term4_assembled_ids,
    STRING_AGG(CASE WHEN impaired_assembled_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(impaired_assembled_cnt AS STRING), '点]', impaired_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS impaired_assembled_ids,
    -- 余りパーツの在庫id（カテゴリ別）- [パーツid:倉庫名:〇点]形式
    STRING_AGG(CASE WHEN term1_leftover_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term1_leftover_cnt AS STRING), '点]', term1_leftover_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term1_leftover_ids,
    STRING_AGG(CASE WHEN term2_leftover_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term2_leftover_cnt AS STRING), '点]', term2_leftover_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term2_leftover_ids,
    STRING_AGG(CASE WHEN term3_leftover_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term3_leftover_cnt AS STRING), '点]', term3_leftover_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term3_leftover_ids,
    STRING_AGG(CASE WHEN term4_leftover_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(term4_leftover_cnt AS STRING), '点]', term4_leftover_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS term4_leftover_ids,
    STRING_AGG(CASE WHEN impaired_leftover_ids IS NOT NULL
      THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ':', CAST(impaired_leftover_cnt AS STRING), '点]', impaired_leftover_ids)
      END, '\n' ORDER BY part_id, warehouse_name) AS impaired_leftover_ids
  FROM sku_stock_ids_by_warehouse
  GROUP BY sku_hash
),

-- ============================================================
-- 7. パーツ単位の在庫数・パーツタイプ情報
-- ============================================================
-- パーツごとにエリア別の在庫数を集計
-- business_area IS NULL（法人直送倉庫）はカウント対象外
part_stock_with_type AS (
  SELECT
    avp.part_id,
    a.type AS part_type,
    -- エリア別在庫数（法人直送倉庫を除く）
    COUNT(CASE WHEN st.business_area = 'Kanto' THEN st.id END) AS cnt_kanto_rec,
    COUNT(CASE WHEN st.business_area = 'Kansai' THEN st.id END) AS cnt_kansai_rec,
    COUNT(CASE WHEN st.business_area = 'Kyushu' THEN st.id END) AS cnt_kyushu_rec
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

sku_product_info AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    sku.deleted_at AS sku_deleted_at,
    pd.id AS product_id,
    se.name AS series_name,
    pd.name AS product_name,
    pd.status AS product_status,
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

attribute_based_info AS (
  SELECT
    TO_HEX(SHA1(ARRAY_TO_STRING(ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.id, leg.id, mr.id, mrt.id, gr.id]) AS element ORDER BY element), ','))) AS sku_hash,
    ARRAY_TO_STRING([body.value, leg.value, mr.value, mrt.value, gr.value], ' ') AS value,
    (SELECT STRING_AGG(DISTINCT suppliers) FROM UNNEST(ARRAY_CONCAT(IFNULL(body.suppliers, []), IFNULL(leg.suppliers, []), IFNULL(mr.suppliers, []), IFNULL(mrt.suppliers, []), IFNULL(gr.suppliers, []))) suppliers) AS suppliers,
    IFNULL(body.cost, 0) + IFNULL(leg.cost, 0) + IFNULL(mr.cost, 0) AS cost,
    body.term1_end AS term1_end,
    body.term2_end AS term2_end,
    body.term3_end AS term3_end
  FROM `lake.series` se
  INNER JOIN `lake.product` pd ON pd.series_id = se.id AND se.deleted_at IS NULL AND pd.deleted_at IS NULL
  LEFT OUTER JOIN (
    SELECT avs.product_id, avs.`type`, avs.value, avs.id, avs.status, avs.suppliers, avs.cost,
           avs.cnt_kanto_rec, avs.cnt_kansai_rec, avs.cnt_kyushu_rec,
           qe.term1_end, qe.term2_end, qe.term3_end
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

dup_sku_with_parts AS (
  SELECT
    sam.sku_id,
    sam.sku_hash,
    sam.product_id,
    sam.product_name,
    sam.series_name,
    sam.guarantee_av_id,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT CAST(avp.part_id AS STRING) ORDER BY CAST(avp.part_id AS STRING)),
      ','
    ) AS part_id_key,
    ARRAY_AGG(DISTINCT avp.part_id ORDER BY avp.part_id) AS part_ids
  FROM dup_sku_attribute_mapping sam
  CROSS JOIN UNNEST([sam.body_av_id, sam.leg_av_id, sam.mattress_av_id, sam.mattress_topper_av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp
    ON avp.attribute_value_id = av_id
    AND avp.deleted_at IS NULL
  WHERE av_id IS NOT NULL
  GROUP BY sam.sku_id, sam.sku_hash, sam.product_id, sam.product_name, sam.series_name, sam.guarantee_av_id
),

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

dup_sku_with_guarantee AS (
  SELECT
    swp.*,
    IFNULL(gi.guarantee_name, '補償なし') AS guarantee_name,
    IFNULL(gi.has_damage_guarantee, FALSE) AS has_damage_guarantee
  FROM dup_sku_with_parts swp
  LEFT JOIN dup_guarantee_info gi ON gi.guarantee_av_id = swp.guarantee_av_id
),

-- SKUを構成するパーツのサプライヤー情報を取得
-- 削除されていないバージョンの中で最大バージョンのサプライヤーを使用
dup_supplier_info AS (
  SELECT
    swg.sku_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT sup.name), ', ') AS suppliers
  FROM dup_sku_with_guarantee swg
  CROSS JOIN UNNEST(swg.part_ids) AS part_id
  -- サプライヤー取得: 削除されていないバージョンの最大を先に特定
  INNER JOIN (
    SELECT pv2.part_id, MAX(pv2.version) AS max_ver
    FROM `lake.part_version` pv2
    WHERE pv2.deleted_at IS NULL  -- ここでフィルタすることが重要
    GROUP BY pv2.part_id
  ) pv_max ON pv_max.part_id = part_id
  INNER JOIN `lake.part_version` pv ON pv.part_id = pv_max.part_id AND pv.version = pv_max.max_ver
  INNER JOIN `lake.supplier` sup ON sup.id = pv.supplier_id AND sup.deleted_at IS NULL
  GROUP BY swg.sku_id
),

dup_sku_full_info AS (
  SELECT
    swg.*,
    IFNULL(si.suppliers, '') AS suppliers,
    CASE
      WHEN IFNULL(si.suppliers, '') LIKE '%法人小物管理用%' THEN TRUE
      ELSE FALSE
    END AS is_corporate_item,
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

dup_duplicate_groups AS (
  SELECT
    part_id_key,
    COUNT(*) AS sku_count
  FROM dup_sku_full_info
  WHERE is_corporate_item = FALSE
    AND is_set_item = FALSE
    AND series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND series_name NOT LIKE '%【CLAS SET】%'
    AND series_name NOT LIKE '%エイトレント%'
    AND product_name NOT LIKE '%プラン%'
  GROUP BY part_id_key
  HAVING COUNT(*) > 1
),

dup_ranked_duplicates AS (
  SELECT
    sfi.sku_id,
    ROW_NUMBER() OVER (
      PARTITION BY sfi.part_id_key
      ORDER BY
        sfi.has_damage_guarantee ASC,
        sfi.sku_id ASC
    ) AS rank_in_group
  FROM dup_duplicate_groups dg
  INNER JOIN dup_sku_full_info sfi
    ON dg.part_id_key = sfi.part_id_key
    AND sfi.is_corporate_item = FALSE
    AND sfi.is_set_item = FALSE
    AND sfi.series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND sfi.series_name NOT LIKE '%【CLAS SET】%'
    AND sfi.series_name NOT LIKE '%エイトレント%'
    AND sfi.product_name NOT LIKE '%プラン%'
),

dup_excluded_skus AS (
  SELECT sku_id FROM dup_ranked_duplicates WHERE rank_in_group > 1
)

-- ============================================================
-- 9. 最終出力（連番方式：1からMAX(SKU_ID)まで全行出力）- 52カラム
-- ============================================================
-- 動的年月カラム名を生成するため、quarter_endsをCROSS JOINする
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
  END AS image_url,  -- 画像リンク

  -- 2. 除外フラグ
  CASE
    WHEN spi.sku_id IS NULL THEN '除外'
    WHEN spi.is_deleted_sku = TRUE THEN '除外'
    WHEN spi.is_orphan_sku = TRUE THEN '除外'
    WHEN IFNULL(abi.suppliers, '') LIKE '%法人小物管理用%' THEN NULL
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
    ELSE NULL
  END AS exclusion_flag,  -- 除外フラグ

  -- 3. SKU_ID
  asi.seq_id AS sku_id,  -- SKU_ID

  -- 4. 商品ID
  spi.product_id AS product_id,  -- 商品ID

  -- 5. 商品IDリンク
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    ELSE CONCAT("https://clas.style/admin/product/", CAST(spi.product_id AS STRING), "#images")
  END AS product_id_link,  -- 商品IDリンク

  -- 6. シリーズ名
  spi.series_name AS series_name,  -- シリーズ名

  -- 7. 商品名
  spi.product_name AS product_name,  -- 商品名

  -- 8. 商品ステータス
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(psm.status_ja, spi.product_status) END AS product_status,  -- 商品ステータス

  -- 9. 属性
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.value, '') END AS attribute_value,  -- 属性

  -- 10. カテゴリ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(cm.category_ja, spi.category) END AS category,  -- カテゴリ

  -- 11. 対象顧客
  spi.customer AS target_customer,  -- 対象顧客

  -- 12. サプライヤー
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.suppliers, '') END AS supplier,  -- サプライヤー

  -- 13. 下代
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(abi.cost, 0) END AS cost_price,  -- 下代

  -- 14. 上代
  spi.retail_price AS retail_price,  -- 上代

  -- 15. 構成パーツid
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.part_ids_str, '') END AS component_part_ids,  -- 構成パーツid

  -- 16. 構成パーツ名
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.part_names_str, '') END AS component_part_names,  -- 構成パーツ名

  -- 17. 構成点数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(spi_info.quantities_str, '') END AS component_quantities,  -- 構成点数

  -- 18. 期末1_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term1_assembled_ids, '') END AS term1_impairment_stock_ids_product,  -- 期末1_減損予定_在庫id_商品

  -- 19. 期末2_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term2_assembled_ids, '') END AS term2_impairment_stock_ids_product,  -- 期末2_減損予定_在庫id_商品

  -- 20. 期末3_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term3_assembled_ids, '') END AS term3_impairment_stock_ids_product,  -- 期末3_減損予定_在庫id_商品

  -- 21. 期末4以降_減損予定_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term4_assembled_ids, '') END AS term4_after_impairment_stock_ids_product,  -- 期末4以降_減損予定_在庫id_商品

  -- 22. 減損済_在庫id_商品
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.impaired_assembled_ids, '') END AS impaired_stock_ids_product,  -- 減損済_在庫id_商品

  -- 23. 期末1_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term1_leftover_ids, '') END AS term1_impairment_stock_ids_leftover,  -- 期末1_減損予定_在庫id_余りパーツ

  -- 24. 期末2_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term2_leftover_ids, '') END AS term2_impairment_stock_ids_leftover,  -- 期末2_減損予定_在庫id_余りパーツ

  -- 25. 期末3_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term3_leftover_ids, '') END AS term3_impairment_stock_ids_leftover,  -- 期末3_減損予定_在庫id_余りパーツ

  -- 26. 期末4以降_減損予定_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.term4_leftover_ids, '') END AS term4_after_impairment_stock_ids_leftover,  -- 期末4以降_減損予定_在庫id_余りパーツ

  -- 27. 減損済_在庫id_余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(stock_ids.impaired_leftover_ids, '') END AS impaired_stock_ids_leftover,  -- 減損済_在庫id_余りパーツ

  -- 28. 期末1_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.term1_product_cnt, 0) END AS term1_impairment_product_count,  -- 期末1_減損予定_商品数

  -- 29. 期末2_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.term2_product_cnt, 0) END AS term2_impairment_product_count,  -- 期末2_減損予定_商品数

  -- 30. 期末3_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.term3_product_cnt, 0) END AS term3_impairment_product_count,  -- 期末3_減損予定_商品数

  -- 31. 期末4以降_減損予定_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.term4_product_cnt, 0) END AS term4_after_impairment_product_count,  -- 期末4以降_減損予定_商品数

  -- 33. 減損済_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.impaired_product_cnt, 0) END AS impaired_product_count,  -- 減損済_商品数

  -- 34. 合計_商品数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sis.total_assemblable, 0) END AS total_product_count,  -- 合計_商品数

  -- 35. 期末1_減損予定_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_term1_total, 0) END AS term1_impairment_leftover_count,  -- 期末1_減損予定_余りパーツ数

  -- 36. 期末2_減損予定_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_term2_total, 0) END AS term2_impairment_leftover_count,  -- 期末2_減損予定_余りパーツ数

  -- 37. 期末3_減損予定_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_term3_total, 0) END AS term3_impairment_leftover_count,  -- 期末3_減損予定_余りパーツ数

  -- 38. 期末4以降_減損予定_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_term4_total, 0) END AS term4_after_impairment_leftover_count,  -- 期末4以降_減損予定_余りパーツ数

  -- 39. 減損済_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.leftover_impaired_total, 0) END AS impaired_leftover_count,  -- 減損済_余りパーツ数

  -- 40. 合計_余りパーツ数
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sls.total_leftover, 0) END AS total_leftover_count,  -- 合計_余りパーツ数

  -- 41. 期末1_減損予定_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.term1_product_cnt, 0) + IFNULL(sls.leftover_term1_total, 0)
  END AS term1_impairment_total,  -- 期末1_減損予定_商品+余りパーツ

  -- 42. 期末2_減損予定_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.term2_product_cnt, 0) + IFNULL(sls.leftover_term2_total, 0)
  END AS term2_impairment_total,  -- 期末2_減損予定_商品+余りパーツ

  -- 43. 期末3_減損予定_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.term3_product_cnt, 0) + IFNULL(sls.leftover_term3_total, 0)
  END AS term3_impairment_total,  -- 期末3_減損予定_商品+余りパーツ

  -- 44. 期末4以降_減損予定_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.term4_product_cnt, 0) + IFNULL(sls.leftover_term4_total, 0)
  END AS term4_after_impairment_total,  -- 期末4以降_減損予定_商品+余りパーツ

  -- 45. 減損済_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.impaired_product_cnt, 0) + IFNULL(sls.leftover_impaired_total, 0)
  END AS impaired_total,  -- 減損済_商品+余りパーツ

  -- 46. 合計_商品+余りパーツ
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.total_assemblable, 0) + IFNULL(sls.total_leftover, 0)
  END AS grand_total,  -- 合計_商品+余りパーツ

  -- 47. 期末1_減損予定_商品+余りパーツ累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sis.term1_product_cnt, 0) + IFNULL(sls.leftover_term1_total, 0)
  END AS term1_impairment_cumulative,  -- 期末1_減損予定_商品+余りパーツ累計

  -- 48. 期末2_減損予定_商品+余りパーツ累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    (IFNULL(sis.term1_product_cnt, 0) + IFNULL(sls.leftover_term1_total, 0)) +
    (IFNULL(sis.term2_product_cnt, 0) + IFNULL(sls.leftover_term2_total, 0))
  END AS term2_impairment_cumulative,  -- 期末2_減損予定_商品+余りパーツ累計

  -- 49. 期末3_減損予定_商品+余りパーツ累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    (IFNULL(sis.term1_product_cnt, 0) + IFNULL(sls.leftover_term1_total, 0)) +
    (IFNULL(sis.term2_product_cnt, 0) + IFNULL(sls.leftover_term2_total, 0)) +
    (IFNULL(sis.term3_product_cnt, 0) + IFNULL(sls.leftover_term3_total, 0))
  END AS term3_impairment_cumulative,  -- 期末3_減損予定_商品+余りパーツ累計

  -- 50. 期末4以降_減損予定_商品+余りパーツ累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    (IFNULL(sis.term1_product_cnt, 0) + IFNULL(sls.leftover_term1_total, 0)) +
    (IFNULL(sis.term2_product_cnt, 0) + IFNULL(sls.leftover_term2_total, 0)) +
    (IFNULL(sis.term3_product_cnt, 0) + IFNULL(sls.leftover_term3_total, 0)) +
    (IFNULL(sis.term4_product_cnt, 0) + IFNULL(sls.leftover_term4_total, 0))
  END AS term4_after_impairment_cumulative,  -- 期末4以降_減損予定_商品+余りパーツ累計

  -- 51. 期末1_減損予定_簿価
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sbv.term1_book_value, 0) END AS term1_impairment_book_value,  -- 期末1_減損予定_簿価

  -- 52. 期末2_減損予定_簿価
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sbv.term2_book_value, 0) END AS term2_impairment_book_value,  -- 期末2_減損予定_簿価

  -- 53. 期末3_減損予定_簿価
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sbv.term3_book_value, 0) END AS term3_impairment_book_value,  -- 期末3_減損予定_簿価

  -- 54. 期末4以降_減損予定_簿価
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE IFNULL(sbv.term4_book_value, 0) END AS term4_after_impairment_book_value,  -- 期末4以降_減損予定_簿価

  -- 55. 期末1_減損予定_簿価_累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sbv.term1_book_value, 0)
  END AS term1_impairment_book_value_cumulative,  -- 期末1_減損予定_簿価_累計

  -- 56. 期末2_減損予定_簿価_累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sbv.term1_book_value, 0) + IFNULL(sbv.term2_book_value, 0)
  END AS term2_impairment_book_value_cumulative,  -- 期末2_減損予定_簿価_累計

  -- 57. 期末3_減損予定_簿価_累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sbv.term1_book_value, 0) + IFNULL(sbv.term2_book_value, 0) + IFNULL(sbv.term3_book_value, 0)
  END AS term3_impairment_book_value_cumulative,  -- 期末3_減損予定_簿価_累計

  -- 58. 期末4以降_減損予定_簿価_累計
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE
    IFNULL(sbv.term1_book_value, 0) + IFNULL(sbv.term2_book_value, 0) + IFNULL(sbv.term3_book_value, 0) + IFNULL(sbv.term4_book_value, 0)
  END AS term4_after_impairment_book_value_cumulative,  -- 期末4以降_減損予定_簿価_累計

  -- 59. 期末1_減損予定_商品_簿価平均単価（分母0の場合は0を返す）
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    WHEN IFNULL(sis.term1_product_cnt, 0) = 0 THEN 0
    ELSE ROUND(IFNULL(sbv.term1_product_book_value, 0) / sis.term1_product_cnt, 0)
  END AS term1_impairment_product_avg_book_value,  -- 期末1_減損予定_商品_簿価平均単価

  -- 60. 期末2_減損予定_商品_簿価平均単価（分母0の場合は0を返す）
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    WHEN IFNULL(sis.term2_product_cnt, 0) = 0 THEN 0
    ELSE ROUND(IFNULL(sbv.term2_product_book_value, 0) / sis.term2_product_cnt, 0)
  END AS term2_impairment_product_avg_book_value,  -- 期末2_減損予定_商品_簿価平均単価

  -- 61. 期末3_減損予定_商品_簿価平均単価（分母0の場合は0を返す）
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    WHEN IFNULL(sis.term3_product_cnt, 0) = 0 THEN 0
    ELSE ROUND(IFNULL(sbv.term3_product_book_value, 0) / sis.term3_product_cnt, 0)
  END AS term3_impairment_product_avg_book_value,  -- 期末3_減損予定_商品_簿価平均単価

  -- 62. 期末4以降_減損予定_商品_簿価平均単価（分母0の場合は0を返す）
  CASE
    WHEN spi.sku_id IS NULL THEN NULL
    WHEN IFNULL(sis.term4_product_cnt, 0) = 0 THEN 0
    ELSE ROUND(IFNULL(sbv.term4_product_book_value, 0) / sis.term4_product_cnt, 0)
  END AS term4_after_impairment_product_avg_book_value,  -- 期末4以降_減損予定_商品_簿価平均単価

  -- 63. 滞留日数_平均_切上
  CASE WHEN spi.sku_id IS NULL THEN NULL ELSE CAST(CEIL(IFNULL(scra.avg_current_retention_days, 0)) AS INT64) END AS avg_retention_days_rounded,  -- 滞留日数_平均_切上

  -- 64-67. 期末日（参照用）
  qe.term1_end AS term1_date,  -- 期末1_日付
  qe.term2_end AS term2_date,  -- 期末2_日付
  qe.term3_end AS term3_date,  -- 期末3_日付
  qe.term4_end AS term4_date   -- 期末4_日付

-- 連番テーブルを起点にLEFT JOIN
FROM all_sku_ids asi
CROSS JOIN quarter_ends qe
LEFT OUTER JOIN sku_product_info spi ON spi.sku_id = asi.seq_id
LEFT OUTER JOIN attribute_based_info abi ON abi.sku_hash = spi.sku_hash
LEFT OUTER JOIN `lake.image` image ON image.type = 'Sku' AND image.ref_id = spi.product_id AND image.hint = spi.sku_hash AND image.deleted_at IS NULL
LEFT OUTER JOIN category_mapping cm ON cm.category_en = spi.category
LEFT OUTER JOIN product_status_mapping psm ON psm.status_en = spi.product_status
LEFT OUTER JOIN sku_current_retention_avg scra ON scra.sku_hash = spi.sku_hash
LEFT OUTER JOIN dup_excluded_skus dup_ex ON dup_ex.sku_id = spi.sku_id
-- 減損情報
LEFT OUTER JOIN sku_impairment_summary sis ON sis.sku_hash = spi.sku_hash
LEFT OUTER JOIN sku_leftover_summary sls ON sls.sku_hash = spi.sku_hash
-- 簿価情報（取得原価から簿価に変更）
LEFT OUTER JOIN sku_book_value_summary sbv ON sbv.sku_hash = spi.sku_hash
-- 構成パーツ情報
LEFT OUTER JOIN sku_part_info spi_info ON spi_info.sku_hash = spi.sku_hash
-- 在庫id詳細（統合CTE）
LEFT OUTER JOIN sku_stock_ids_unified stock_ids ON stock_ids.sku_hash = spi.sku_hash

-- SKU_ID昇順で並び替え
ORDER BY asi.seq_id ASC
;
