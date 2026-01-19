-- ============================================================
-- KINFORT ソファ サマリークエリ
-- ============================================================
-- 目的: 商品コードごとに在庫状況を一覧表示
-- 指摘事項との照合用
-- ============================================================

WITH target_products AS (
  SELECT id AS product_id, code AS product_code, name AS product_name
  FROM `lake.product`
  WHERE deleted_at IS NULL
    AND code IN ('504-5129', '504-5137', '504-5105', '504-5101', '504-5117')
),

attribute_value_by_type AS (
  SELECT a.product_id, a.type, av.id AS av_id
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
),

sku_parts AS (
  SELECT DISTINCT
    tp.product_code,
    tp.product_name,
    avp.part_id,
    p.name AS part_name,
    avp.quantity AS required_qty
  FROM target_products tp
  LEFT JOIN attribute_value_by_type body ON body.product_id = tp.product_id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = tp.product_id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = tp.product_id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = tp.product_id AND mrt.type = 'MattressTopper'
  CROSS JOIN UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id AND avp.deleted_at IS NULL
  INNER JOIN `lake.part` p ON p.id = avp.part_id AND p.deleted_at IS NULL
  WHERE av_id IS NOT NULL
),

-- 全在庫 (削除されていないもの全て)
all_stock AS (
  SELECT sp.*, s.id AS stock_id, s.status, s._rank_
  FROM sku_parts sp
  INNER JOIN `lake.stock` s ON s.part_id = sp.part_id AND s.deleted_at IS NULL
),

-- ダッシュボード対象 (Ready/Waiting/Recovery、ランクR/L除外)
dashboard_stock AS (
  SELECT sp.*, s.id AS stock_id, s.status, s._rank_, w.business_area
  FROM sku_parts sp
  INNER JOIN `lake.stock` s ON s.part_id = sp.part_id AND s.deleted_at IS NULL
  INNER JOIN `lake.part` p ON p.id = s.part_id AND p.deleted_at IS NULL
  INNER JOIN `lake.part_version` pv ON pv.id = s.part_version_id AND pv.deleted_at IS NULL
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
  -- 除外: toC引当準備中
  LEFT JOIN (
    SELECT ld.stock_id FROM `lake.lent` l INNER JOIN `lake.lent_detail` ld ON l.id = ld.lent_id
    WHERE l.status IN ('Preparing', 'Adjusting') AND l.deleted_at IS NULL AND ld.deleted_at IS NULL
    GROUP BY ld.stock_id
  ) to_c ON to_c.stock_id = s.id
  -- 除外: toB引当準備中
  LEFT JOIN (
    SELECT d.stock_id FROM `lake.lent_to_b` b INNER JOIN `lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing' AND b.deleted_at IS NULL AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  WHERE (s.status IN ('Ready', 'Waiting') OR (s.status = 'Recovery' AND s.part_id NOT IN (7108,7109,7110,7438,7439,7440,7570,7571,7572,7573)))
    AND to_c.stock_id IS NULL
    AND to_b.stock_id IS NULL
    AND w.available_for_business = TRUE
    AND s._rank_ NOT IN ('R', 'L')
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
),

-- 商品コードごとのサマリー
summary AS (
  SELECT
    sp.product_code,
    sp.product_name,
    sp.part_id,
    sp.part_name,
    sp.required_qty,
    -- 全在庫数
    COUNT(DISTINCT ast.stock_id) AS total_stock,
    -- ステータス別
    COUNT(DISTINCT CASE WHEN ast.status = 'Ready' THEN ast.stock_id END) AS ready_cnt,
    COUNT(DISTINCT CASE WHEN ast.status = 'Waiting' THEN ast.stock_id END) AS waiting_cnt,
    COUNT(DISTINCT CASE WHEN ast.status = 'Recovery' THEN ast.stock_id END) AS recovery_cnt,
    COUNT(DISTINCT CASE WHEN ast.status = 'Lent' THEN ast.stock_id END) AS lent_cnt,
    COUNT(DISTINCT CASE WHEN ast.status = 'LentToB' THEN ast.stock_id END) AS lent_to_b_cnt,
    COUNT(DISTINCT CASE WHEN ast.status NOT IN ('Ready', 'Waiting', 'Recovery', 'Lent', 'LentToB') THEN ast.stock_id END) AS other_cnt,
    -- ランク別
    COUNT(DISTINCT CASE WHEN ast._rank_ IN ('R', 'L') THEN ast.stock_id END) AS rank_rl_cnt,
    -- ダッシュボード対象
    COUNT(DISTINCT ds.stock_id) AS dashboard_eligible
  FROM sku_parts sp
  LEFT JOIN all_stock ast ON sp.product_code = ast.product_code AND sp.part_id = ast.part_id
  LEFT JOIN dashboard_stock ds ON sp.product_code = ds.product_code AND sp.part_id = ds.part_id
  GROUP BY sp.product_code, sp.product_name, sp.part_id, sp.part_name, sp.required_qty
)

SELECT
  product_code,
  product_name,
  part_id,
  part_name,
  required_qty AS 必要数,
  total_stock AS 全在庫,
  ready_cnt AS Ready,
  waiting_cnt AS Waiting,
  recovery_cnt AS Recovery,
  lent_cnt AS Lent_toC,
  lent_to_b_cnt AS Lent_toB,
  other_cnt AS その他,
  rank_rl_cnt AS ランクR_L,
  dashboard_eligible AS ダッシュボード対象,
  CAST(dashboard_eligible / required_qty AS INT64) AS 組み上げ可能数
FROM summary
ORDER BY product_code, part_id;
