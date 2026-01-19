-- ============================================================
-- KINFORT ソファ 検証クエリ
-- ============================================================
-- 目的: Slackで指摘された商品コードの在庫状況を確認
-- 対象商品:
--   504-5129: KINFORTソファ2.5P セサミブラウン
--   504-5137: KINFORTソファ2.5P チャコールグレー
--   504-5105: KINFORTソファ2P ペブルグレー
--   504-5101: KINFORTソファ2P ユーカリグリーン
--   504-5117: KINFORTソファ2P チャコールグレー
-- ============================================================

-- ============================================================
-- 1. 対象SKUの基本情報を確認
-- ============================================================
WITH target_skus AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    sku.product_id,
    pd.name AS product_name,
    pd.code AS product_code
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id
  WHERE pd.deleted_at IS NULL
    AND sku.deleted_at IS NULL
    AND pd.code IN ('504-5129', '504-5137', '504-5105', '504-5101', '504-5117')
),

-- ============================================================
-- 2. SKUを構成するパーツ情報
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

sku_parts AS (
  SELECT
    ts.sku_id,
    ts.product_code,
    ts.product_name,
    avp.part_id,
    p.name AS part_name,
    avp.quantity AS required_quantity
  FROM target_skus ts
  LEFT JOIN attribute_value_by_type body ON body.product_id = ts.product_id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = ts.product_id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = ts.product_id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = ts.product_id AND mrt.type = 'MattressTopper'
  CROSS JOIN UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id AND avp.deleted_at IS NULL
  INNER JOIN `lake.part` p ON p.id = avp.part_id AND p.deleted_at IS NULL
  WHERE av_id IS NOT NULL
),

-- ============================================================
-- 3. 各パーツの在庫状況 (全ステータス)
-- ============================================================
part_stock_all AS (
  SELECT
    sp.product_code,
    sp.product_name,
    sp.part_id,
    sp.part_name,
    sp.required_quantity,
    s.id AS stock_id,
    s.status,
    s._rank_ AS stock_rank,
    w.name AS warehouse_name,
    w.business_area,
    s.arrival_at,
    s.impairment_date
  FROM sku_parts sp
  INNER JOIN `lake.stock` s ON s.part_id = sp.part_id AND s.deleted_at IS NULL
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
),

-- ============================================================
-- 4. ダッシュボード対象となる在庫 (stocks CTE の条件を再現)
-- ============================================================
dashboard_eligible_stock AS (
  SELECT
    sp.product_code,
    sp.product_name,
    sp.part_id,
    sp.part_name,
    sp.required_quantity,
    s.id AS stock_id,
    s.status,
    s._rank_ AS stock_rank,
    w.name AS warehouse_name,
    w.business_area
  FROM sku_parts sp
  INNER JOIN `lake.stock` s ON s.part_id = sp.part_id AND s.deleted_at IS NULL
  INNER JOIN `lake.part` p ON p.id = s.part_id AND p.deleted_at IS NULL
  INNER JOIN `lake.part_version` pv ON pv.id = s.part_version_id AND pv.deleted_at IS NULL
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
  -- toC引当準備中/調整中の在庫を除外
  LEFT OUTER JOIN (
    SELECT ld.stock_id
    FROM `lake.lent` l INNER JOIN `lake.lent_detail` ld ON l.id = ld.lent_id
    WHERE l.status IN ('Preparing', 'Adjusting') AND l.deleted_at IS NULL AND ld.deleted_at IS NULL
    GROUP BY ld.stock_id
  ) to_c ON to_c.stock_id = s.id
  -- toB引当準備中の在庫を除外
  LEFT OUTER JOIN (
    SELECT d.stock_id
    FROM `lake.lent_to_b` b INNER JOIN `lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing' AND b.deleted_at IS NULL AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  -- メモタグ除外
  LEFT OUTER JOIN (
    SELECT sm.stock_id
    FROM `lake.staff_memo` sm
    INNER JOIN `lake.staff_memo_tag` smt ON sm.id = smt.staff_memo_id AND sm.stock_id IS NOT NULL
    INNER JOIN `lake.memo_tag` mt ON mt.id = smt.memo_tag_id AND mt.id = 64
    WHERE sm.deleted_at IS NULL AND smt.deleted_at IS NULL AND mt.deleted_at IS NULL
    GROUP BY sm.stock_id
  ) tag ON tag.stock_id = s.id
  -- B向け発注済み在庫を除外
  LEFT OUTER JOIN (
    SELECT pds.stock_id
    FROM `lake.purchasing_detail_stock` pds
    INNER JOIN `lake.purchasing_detail` pd ON pds.purchasing_detail_id = pd.id
    INNER JOIN `lake.purchasing` p ON p.id = pd.purchasing_id
    WHERE pds.deleted_at IS NULL AND pd.deleted_at IS NULL AND p.deleted_at IS NULL
      AND p.status = 'Done' AND p.orderer_email = 'info+b-order@clas.style'
    GROUP BY pds.stock_id
  ) purchase ON purchase.stock_id = s.id
  -- 外部販売中の在庫を除外
  LEFT OUTER JOIN (
    SELECT xss.stock_id
    FROM `lake.external_sale_stock` xss
    INNER JOIN `lake.external_sale_product` xsp ON xsp.id = xss.external_sale_product_id
    WHERE xss.deleted_at IS NULL AND xsp.deleted_at IS NULL AND xsp.status != 'Deny'
    GROUP BY xss.stock_id
  ) external_sale ON external_sale.stock_id = s.id
  WHERE
    -- ステータス条件
    (s.status IN ('Ready', 'Waiting') OR (s.status = 'Recovery' AND s.part_id NOT IN (7108,7109,7110,7438,7439,7440,7570,7571,7572,7573)))
    AND tag.stock_id IS NULL
    AND to_c.stock_id IS NULL
    AND to_b.stock_id IS NULL
    AND w.available_for_business = TRUE
    AND s._rank_ NOT IN ('R', 'L')  -- ランクR/Lは除外
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
    AND external_sale.stock_id IS NULL
)

-- ============================================================
-- 結果1: 全在庫のステータス別サマリー
-- ============================================================
SELECT
  '1. 全在庫ステータス別' AS query_type,
  product_code,
  product_name,
  part_id,
  part_name,
  required_quantity,
  status,
  stock_rank,
  COUNT(*) AS stock_count
FROM part_stock_all
GROUP BY product_code, product_name, part_id, part_name, required_quantity, status, stock_rank
ORDER BY product_code, part_id, status;

-- ============================================================
-- 結果2: ダッシュボード対象の在庫サマリー
-- ============================================================
-- SELECT
--   '2. ダッシュボード対象' AS query_type,
--   product_code,
--   product_name,
--   part_id,
--   part_name,
--   required_quantity,
--   business_area,
--   COUNT(*) AS eligible_stock_count
-- FROM dashboard_eligible_stock
-- GROUP BY product_code, product_name, part_id, part_name, required_quantity, business_area
-- ORDER BY product_code, part_id, business_area;

-- ============================================================
-- 結果3: 組み上げ可能数の計算
-- ============================================================
-- SELECT
--   product_code,
--   product_name,
--   business_area,
--   MIN(CAST(eligible_stock_count / required_quantity AS INT64)) AS assemblable_count
-- FROM (
--   SELECT
--     product_code,
--     product_name,
--     part_id,
--     required_quantity,
--     business_area,
--     COUNT(*) AS eligible_stock_count
--   FROM dashboard_eligible_stock
--   GROUP BY product_code, product_name, part_id, required_quantity, business_area
-- )
-- GROUP BY product_code, product_name, business_area
-- ORDER BY product_code, business_area;
