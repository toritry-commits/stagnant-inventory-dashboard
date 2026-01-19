-- ============================================================
-- KINFORT ソファ 貸出状況確認クエリ
-- ============================================================
-- 目的: 貸出中の在庫がどれだけあるかを確認
-- ============================================================

-- ============================================================
-- 1. 対象SKUの構成パーツを取得
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
    avp.quantity AS required_quantity
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

-- ============================================================
-- 2. 全在庫とステータスを取得
-- ============================================================
all_stock AS (
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
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
),

-- ============================================================
-- 3. toC貸出中の在庫を確認
-- ============================================================
toc_lent AS (
  SELECT
    ld.stock_id,
    l.status AS lent_status,
    l.shipped_at,
    l.return_at
  FROM `lake.lent` l
  INNER JOIN `lake.lent_detail` ld ON l.id = ld.lent_id
  WHERE l.deleted_at IS NULL AND ld.deleted_at IS NULL
    AND l.status NOT IN ('Cancel', 'PurchaseFailed', 'Returned')
),

-- ============================================================
-- 4. toB貸出中の在庫を確認
-- ============================================================
tob_lent AS (
  SELECT
    d.stock_id,
    b.status AS lent_status,
    b.start_at,
    b.end_at
  FROM `lake.lent_to_b` b
  INNER JOIN `lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
  WHERE b.deleted_at IS NULL AND d.deleted_at IS NULL
    AND b.status NOT IN ('Cancel', 'Ended')
)

-- ============================================================
-- 結果: 在庫ごとの詳細状況
-- ============================================================
SELECT
  ast.product_code,
  ast.product_name,
  ast.part_id,
  ast.part_name,
  ast.required_quantity,
  ast.stock_id,
  ast.status AS stock_status,
  ast.stock_rank,
  ast.warehouse_name,
  ast.business_area,
  CASE
    WHEN toc.stock_id IS NOT NULL THEN CONCAT('toC貸出中: ', toc.lent_status)
    WHEN tob.stock_id IS NOT NULL THEN CONCAT('toB貸出中: ', tob.lent_status)
    ELSE NULL
  END AS lent_info,
  tob.start_at AS tob_start_at
FROM all_stock ast
LEFT JOIN toc_lent toc ON ast.stock_id = toc.stock_id
LEFT JOIN tob_lent tob ON ast.stock_id = tob.stock_id
ORDER BY ast.product_code, ast.part_id, ast.stock_id;
