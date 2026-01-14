-- ============================================================
-- 滞留在庫ダッシュボード: 組み上げ商品 (term別縦持ち)
-- ============================================================
-- 作成日: 2026-01-15
-- 基盤: stagnant_inventory_dashboard_v4.sql
--
-- 出力形式: SKU情報 + term別の商品数・簿価 (縦持ち)
-- 用途: 減損リスク分析、term別フィルタリング、グラフ化
--
-- 出力カラム:
--   - SKU基本情報: sku_id, sku_hash, product_name, series_name, category, product_status
--   - 滞留情報: avg_retention_days
--   - 期末日付: term_end_date
--   - term情報: term (term1/term2/term3/term4/impaired)
--   - 数量・金額: product_cnt, book_value
--   - 構成パーツ: part_ids, part_names, part_quantities
-- ============================================================

WITH
-- ============================================================
-- 0. SKU_IDの連番を生成
-- ============================================================
max_sku_id AS (
  SELECT MAX(id) AS max_id FROM `lake.sku`
),
all_sku_ids AS (
  SELECT seq_id
  FROM max_sku_id, UNNEST(GENERATE_ARRAY(1, max_id)) AS seq_id
),

-- ============================================================
-- 0-1. 属性タイプ別のattribute_value
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
-- 1. 期末日付の動的計算
-- ============================================================
quarter_ends AS (
  SELECT
    CURRENT_DATE() AS today,
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
-- 1-2. 簿価データ取得 (mart.monthly_stock_valuation)
-- ============================================================
latest_period AS (
  SELECT MAX(period_end) AS period_end
  FROM `mart.monthly_stock_valuation`
),

stock_book_value_latest AS (
  SELECT
    msv.stock_id,
    msv.book_value_closing
  FROM `mart.monthly_stock_valuation` msv
  INNER JOIN latest_period lp ON msv.period_end = lp.period_end
),

stock_book_value_by_term AS (
  SELECT
    msv.stock_id,
    qe.term1_end,
    qe.term2_end,
    qe.term3_end,
    qe.term4_end,
    MAX(CASE WHEN msv.period_end <= qe.term1_end THEN msv.book_value_closing END) AS book_value_term1,
    MAX(CASE WHEN msv.period_end <= qe.term2_end THEN msv.book_value_closing END) AS book_value_term2,
    MAX(CASE WHEN msv.period_end <= qe.term3_end THEN msv.book_value_closing END) AS book_value_term3,
    MAX(CASE WHEN msv.period_end <= qe.term4_end THEN msv.book_value_closing END) AS book_value_term4
  FROM `mart.monthly_stock_valuation` msv
  CROSS JOIN quarter_ends qe
  GROUP BY msv.stock_id, qe.term1_end, qe.term2_end, qe.term3_end, qe.term4_end
),

-- ============================================================
-- 2. 引当可能な在庫
-- ============================================================
stocks AS (
  SELECT
    s.id,
    s.status,
    s.part_id,
    s.part_version_id,
    s.supplier_id,
    w.business_area,
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
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
    AND external_sale.stock_id IS NULL
),

-- ============================================================
-- 3. 滞留日数計算用CTE
-- ============================================================
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    s.arrival_at
  FROM `lake.stock` s
  WHERE s.deleted_at IS NULL
),

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

first_b2b_ship AS (
  SELECT
    detail.stock_id,
    MIN(lent_b.start_at) AS first_start_at
  FROM `lake.lent_to_b` lent_b
  INNER JOIN `lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status NOT IN ('Cancel') AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL
  GROUP BY detail.stock_id
),

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

-- ============================================================
-- 4. SKU→パーツマッピング
-- ============================================================
sku_part_detail AS (
  SELECT DISTINCT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    sku.product_id,
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

sku_part_info AS (
  SELECT
    sku_hash,
    STRING_AGG(CAST(part_id AS STRING), '\n' ORDER BY part_id) AS part_ids_str,
    STRING_AGG(part_name, '\n' ORDER BY part_id) AS part_names_str,
    STRING_AGG(CAST(quantity AS STRING), '\n' ORDER BY part_id) AS quantities_str
  FROM sku_part_detail
  GROUP BY sku_hash
),

sku_required_parts AS (
  SELECT
    sku_hash,
    COUNT(DISTINCT part_id) AS required_part_count
  FROM sku_part_detail
  GROUP BY sku_hash
),

-- ============================================================
-- 5. SKU優先順位の決定
-- ============================================================
sku_rental_stats AS (
  SELECT
    CAST(ltb.sku_id AS INT64) AS sku_id,
    COUNT(DISTINCT ltb.id) AS rental_count_30d,
    MAX(ltb.created_at) AS last_rental_date
  FROM `lake.lent_to_b` ltb
  WHERE ltb.deleted_at IS NULL
    AND ltb.status NOT IN ('Cancel')
    AND CAST(ltb.created_at AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY CAST(ltb.sku_id AS INT64)
),

sku_with_priority AS (
  SELECT
    spd.sku_id,
    spd.sku_hash,
    spd.product_id,
    COALESCE(srs.rental_count_30d, 0) AS rental_count_30d,
    srs.last_rental_date,
    DENSE_RANK() OVER (
      ORDER BY
        COALESCE(srs.rental_count_30d, 0) DESC,
        srs.last_rental_date DESC NULLS LAST,
        spd.sku_id ASC
    ) AS sku_priority
  FROM (SELECT DISTINCT sku_id, sku_hash, product_id FROM sku_part_detail) spd
  LEFT JOIN sku_rental_stats srs ON spd.sku_id = srs.sku_id
),

-- ============================================================
-- 6. 在庫に滞留開始日とカテゴリを付与
-- ============================================================
stock_retention_start AS (
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

stock_with_category AS (
  SELECT
    srs.stock_id,
    srs.part_id,
    srs.business_area,
    srs.warehouse_name,
    srs.retention_start_date,
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 'impaired'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 'impaired'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 'term1'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 'term2'
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 'term3'
      ELSE 'term4'
    END AS category,
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 1
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 2
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 3
      ELSE 4
    END AS category_order,
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN COALESCE(sbvl.book_value_closing, srs.cost)
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN COALESCE(sbvl.book_value_closing, srs.cost)
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN COALESCE(sbvt.book_value_term1, sbvl.book_value_closing, srs.cost)
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN COALESCE(sbvt.book_value_term2, sbvl.book_value_closing, srs.cost)
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN COALESCE(sbvt.book_value_term3, sbvl.book_value_closing, srs.cost)
      ELSE COALESCE(sbvt.book_value_term4, sbvl.book_value_closing, srs.cost)
    END AS book_value
  FROM stock_retention_start srs
  CROSS JOIN quarter_ends qe
  LEFT JOIN stock_book_value_latest sbvl ON srs.stock_id = sbvl.stock_id
  LEFT JOIN stock_book_value_by_term sbvt ON srs.stock_id = sbvt.stock_id
),

-- ============================================================
-- 7. パーツ×エリア別在庫集計
-- ============================================================
stock_ordered AS (
  SELECT
    swc.stock_id,
    swc.part_id,
    swc.business_area,
    swc.warehouse_name,
    swc.category,
    swc.category_order,
    swc.book_value,
    ROW_NUMBER() OVER (
      PARTITION BY swc.part_id, swc.business_area
      ORDER BY
        swc.category_order ASC,
        swc.stock_id ASC
    ) AS stock_rank
  FROM stock_with_category swc
  WHERE swc.business_area IS NOT NULL
),

part_area_inventory AS (
  SELECT
    part_id,
    business_area,
    COUNT(*) AS stock_count
  FROM stock_ordered
  GROUP BY part_id, business_area
),

-- ============================================================
-- 8. SKU×エリアごとの組み上げ可能数
-- ============================================================
all_areas AS (
  SELECT DISTINCT business_area FROM stock_ordered WHERE business_area IS NOT NULL
),

sku_part_with_inventory AS (
  SELECT
    spd.sku_id,
    spd.sku_hash,
    spd.product_id,
    spd.part_id,
    spd.quantity AS required_qty,
    swp.sku_priority,
    areas.business_area,
    COALESCE(pai.stock_count, 0) AS stock_count,
    CAST(FLOOR(COALESCE(pai.stock_count, 0) / spd.quantity) AS INT64) AS max_units_from_part
  FROM sku_part_detail spd
  INNER JOIN sku_with_priority swp ON spd.sku_id = swp.sku_id
  CROSS JOIN all_areas areas
  LEFT JOIN part_area_inventory pai
    ON spd.part_id = pai.part_id AND areas.business_area = pai.business_area
),

sku_area_max_assemblable AS (
  SELECT
    sku_id,
    sku_hash,
    product_id,
    business_area,
    sku_priority,
    MIN(max_units_from_part) AS max_assemblable
  FROM sku_part_with_inventory
  GROUP BY sku_id, sku_hash, product_id, business_area, sku_priority
  HAVING MIN(max_units_from_part) > 0
),

-- ============================================================
-- 9. 排他的割り当て: 累積消費量計算
-- ============================================================
sku_cumulative_consumption AS (
  SELECT
    sama.sku_id,
    sama.sku_hash,
    sama.product_id,
    sama.business_area,
    sama.sku_priority,
    sama.max_assemblable,
    spd.part_id,
    spd.quantity AS required_qty,
    SUM(sama.max_assemblable * spd.quantity) OVER (
      PARTITION BY spd.part_id, sama.business_area
      ORDER BY sama.sku_priority, sama.sku_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_required,
    pai.stock_count AS available_count
  FROM sku_area_max_assemblable sama
  INNER JOIN sku_part_detail spd ON sama.sku_hash = spd.sku_hash
  LEFT JOIN part_area_inventory pai
    ON spd.part_id = pai.part_id AND sama.business_area = pai.business_area
),

sku_part_allocatable AS (
  SELECT
    sku_id,
    sku_hash,
    product_id,
    business_area,
    sku_priority,
    max_assemblable,
    part_id,
    required_qty,
    cumulative_required,
    available_count,
    CASE
      WHEN cumulative_required <= COALESCE(available_count, 0) THEN max_assemblable
      WHEN cumulative_required - max_assemblable * required_qty < COALESCE(available_count, 0) THEN
        CAST(FLOOR((COALESCE(available_count, 0) - (cumulative_required - max_assemblable * required_qty)) / required_qty) AS INT64)
      ELSE 0
    END AS allocatable_units
  FROM sku_cumulative_consumption
),

sku_final_assemblable AS (
  SELECT
    sku_id,
    sku_hash,
    product_id,
    business_area,
    sku_priority,
    MIN(allocatable_units) AS final_units
  FROM sku_part_allocatable
  GROUP BY sku_id, sku_hash, product_id, business_area, sku_priority
  HAVING MIN(allocatable_units) > 0
),

-- ============================================================
-- 10. 実際の在庫割り当て
-- ============================================================
sku_units_base AS (
  SELECT
    sfa.sku_id,
    sfa.sku_hash,
    sfa.product_id,
    sfa.sku_priority,
    sfa.business_area,
    unit_no
  FROM sku_final_assemblable sfa
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, sfa.final_units)) AS unit_no
),

sku_units_ranked AS (
  SELECT
    sub.*,
    ROW_NUMBER() OVER (
      PARTITION BY sub.business_area
      ORDER BY sub.sku_priority, sub.sku_id, sub.unit_no
    ) AS unit_rank
  FROM sku_units_base sub
),

sku_unit_parts AS (
  SELECT
    sur.sku_id,
    sur.sku_hash,
    sur.product_id,
    sur.sku_priority,
    sur.business_area,
    sur.unit_no,
    sur.unit_rank,
    spd.part_id,
    spd.quantity AS qty_per_unit
  FROM sku_units_ranked sur
  INNER JOIN sku_part_detail spd ON sur.sku_hash = spd.sku_hash
),

sku_unit_consumption AS (
  SELECT
    sup.*,
    COALESCE(
      SUM(sup.qty_per_unit) OVER (
        PARTITION BY sup.business_area, sup.part_id
        ORDER BY sup.unit_rank
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS consumed_before,
    SUM(sup.qty_per_unit) OVER (
      PARTITION BY sup.business_area, sup.part_id
      ORDER BY sup.unit_rank
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS consumed_after
  FROM sku_unit_parts sup
),

unit_stock_assignment AS (
  SELECT
    suc.sku_id,
    suc.sku_hash,
    suc.product_id,
    suc.business_area,
    suc.unit_no,
    suc.part_id,
    so.stock_id,
    so.category,
    so.category_order,
    so.warehouse_name,
    so.book_value
  FROM sku_unit_consumption suc
  INNER JOIN stock_ordered so
    ON so.business_area = suc.business_area
    AND so.part_id = suc.part_id
    AND so.stock_rank > suc.consumed_before
    AND so.stock_rank <= suc.consumed_after
),

-- ============================================================
-- 11. 商品ごとの減損カテゴリ判定
-- ============================================================
product_with_category AS (
  SELECT
    sku_id,
    sku_hash,
    product_id,
    business_area,
    unit_no,
    MIN(CASE WHEN category != 'impaired' THEN category_order END) AS earliest_order,
    CASE WHEN MIN(CASE WHEN category != 'impaired' THEN category_order END) IS NULL THEN TRUE ELSE FALSE END AS all_impaired,
    STRING_AGG(CAST(stock_id AS STRING), ',' ORDER BY stock_id) AS stock_ids,
    STRING_AGG(DISTINCT warehouse_name, ',' ORDER BY warehouse_name) AS warehouses,
    SUM(book_value) AS total_book_value
  FROM unit_stock_assignment
  GROUP BY sku_id, sku_hash, product_id, business_area, unit_no
),

product_categorized AS (
  SELECT
    pwc.*,
    CASE
      WHEN pwc.all_impaired THEN 'impaired'
      WHEN pwc.earliest_order = 1 THEN 'term1'
      WHEN pwc.earliest_order = 2 THEN 'term2'
      WHEN pwc.earliest_order = 3 THEN 'term3'
      WHEN pwc.earliest_order = 4 THEN 'term4'
    END AS product_category
  FROM product_with_category pwc
),

-- ============================================================
-- 12. SKU別商品数集計 (横持ち)
-- ============================================================
sku_product_summary AS (
  SELECT
    sku_hash,
    COUNT(*) AS total_assemblable,
    SUM(CASE WHEN product_category = 'impaired' THEN 1 ELSE 0 END) AS impaired_product_cnt,
    SUM(CASE WHEN product_category = 'term1' THEN 1 ELSE 0 END) AS term1_product_cnt,
    SUM(CASE WHEN product_category = 'term2' THEN 1 ELSE 0 END) AS term2_product_cnt,
    SUM(CASE WHEN product_category = 'term3' THEN 1 ELSE 0 END) AS term3_product_cnt,
    SUM(CASE WHEN product_category = 'term4' THEN 1 ELSE 0 END) AS term4_product_cnt,
    SUM(total_book_value) AS total_book_value,
    SUM(CASE WHEN product_category = 'impaired' THEN total_book_value ELSE 0 END) AS impaired_book_value,
    SUM(CASE WHEN product_category = 'term1' THEN total_book_value ELSE 0 END) AS term1_book_value,
    SUM(CASE WHEN product_category = 'term2' THEN total_book_value ELSE 0 END) AS term2_book_value,
    SUM(CASE WHEN product_category = 'term3' THEN total_book_value ELSE 0 END) AS term3_book_value,
    SUM(CASE WHEN product_category = 'term4' THEN total_book_value ELSE 0 END) AS term4_book_value
  FROM product_categorized
  GROUP BY sku_hash
),

-- ============================================================
-- 13. 平均滞留日数
-- ============================================================
sku_part_mapping AS (
  SELECT DISTINCT sku_hash, part_id FROM sku_part_detail
),

sku_current_retention_avg AS (
  SELECT
    spm.sku_hash,
    ROUND(AVG(pcra.avg_current_retention_days), 1) AS avg_current_retention_days
  FROM sku_part_mapping spm
  LEFT JOIN part_current_retention_avg pcra ON spm.part_id = pcra.part_id
  GROUP BY spm.sku_hash
),

-- ============================================================
-- 14. ステータス・カテゴリマッピング
-- ============================================================
product_status_mapping AS (
  SELECT status_en, status_ja FROM UNNEST([
    STRUCT('OnSale' AS status_en, '販売中' AS status_ja),
    STRUCT('Soldout', '売り切れ'),
    STRUCT('Stopped', '販売停止')
  ])
),

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
-- 15. 横持ちデータ作成
-- ============================================================
sku_horizontal AS (
  SELECT
    asi.seq_id AS sku_id,
    sku.hash AS sku_hash,
    pd.name AS product_name,
    se.name AS series_name,
    COALESCE(cm.category_ja, se.category) AS category,
    COALESCE(psm.status_ja, pd.status) AS product_status,
    COALESCE(scra.avg_current_retention_days, 0) AS avg_retention_days,
    qe.term1_end,
    qe.term2_end,
    qe.term3_end,
    qe.term4_end,
    COALESCE(sps.total_assemblable, 0) AS total_product_cnt,
    COALESCE(sps.impaired_product_cnt, 0) AS impaired_product_cnt,
    COALESCE(sps.term1_product_cnt, 0) AS term1_product_cnt,
    COALESCE(sps.term2_product_cnt, 0) AS term2_product_cnt,
    COALESCE(sps.term3_product_cnt, 0) AS term3_product_cnt,
    COALESCE(sps.term4_product_cnt, 0) AS term4_product_cnt,
    COALESCE(sps.total_book_value, 0) AS total_book_value,
    COALESCE(sps.impaired_book_value, 0) AS impaired_book_value,
    COALESCE(sps.term1_book_value, 0) AS term1_book_value,
    COALESCE(sps.term2_book_value, 0) AS term2_book_value,
    COALESCE(sps.term3_book_value, 0) AS term3_book_value,
    COALESCE(sps.term4_book_value, 0) AS term4_book_value,
    spi.part_ids_str AS part_ids,
    spi.part_names_str AS part_names,
    spi.quantities_str AS part_quantities
  FROM all_sku_ids asi
  LEFT JOIN `lake.sku` sku ON sku.id = asi.seq_id AND sku.deleted_at IS NULL
  LEFT JOIN `lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  LEFT JOIN `lake.series` se ON se.id = pd.series_id AND se.deleted_at IS NULL
  LEFT JOIN category_mapping cm ON cm.category_en = se.category
  LEFT JOIN product_status_mapping psm ON psm.status_en = pd.status
  LEFT JOIN sku_product_summary sps ON sps.sku_hash = sku.hash
  LEFT JOIN sku_current_retention_avg scra ON scra.sku_hash = sku.hash
  LEFT JOIN sku_part_info spi ON spi.sku_hash = sku.hash
  CROSS JOIN quarter_ends qe
),

-- ============================================================
-- 16. 縦持ち変換 (UNPIVOT)
-- ============================================================
final_output AS (
  SELECT
    sku_id,
    sku_hash,
    product_name,
    series_name,
    category,
    product_status,
    avg_retention_days,
    term,
    term_end_date,
    product_cnt,
    book_value,
    part_ids,
    part_names,
    part_quantities
  FROM sku_horizontal
  CROSS JOIN UNNEST([
    STRUCT('impaired' AS term, CAST(NULL AS DATE) AS term_end_date, impaired_product_cnt AS product_cnt, impaired_book_value AS book_value),
    STRUCT('term1', term1_end, term1_product_cnt, term1_book_value),
    STRUCT('term2', term2_end, term2_product_cnt, term2_book_value),
    STRUCT('term3', term3_end, term3_product_cnt, term3_book_value),
    STRUCT('term4', term4_end, term4_product_cnt, term4_book_value)
  ])
  WHERE total_product_cnt > 0
)

SELECT * FROM final_output
ORDER BY sku_id ASC,
  CASE term
    WHEN 'impaired' THEN 0
    WHEN 'term1' THEN 1
    WHEN 'term2' THEN 2
    WHEN 'term3' THEN 3
    WHEN 'term4' THEN 4
  END;
