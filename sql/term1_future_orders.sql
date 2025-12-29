-- ============================================================================
-- 2026年2月末減損対象 × 未来案件紐づき在庫の固定資産台帳出力
-- ============================================================================
-- 目的:
--   2026年2月末で減損対象となる在庫のうち、法人/EC案件で未来日付（2026年3月以降）
--   に紐づいているものを固定資産台帳形式で出力する。
--
-- 用途:
--   監査法人との協議資料（未来案件に紐づいている在庫を減損対象外にできるか確認）
--
-- 作成日: 2025-12-26
-- ============================================================================

WITH
-- ============================================================================
-- 1. 期末日の設定（2026年2月28日）
-- ============================================================================
target_term AS (
  SELECT DATE('2026-02-28') AS term_end
),

-- ============================================================================
-- 2. 全在庫（減損対象かどうかに関わらず、未来案件への紐づきを確認するため）
-- ============================================================================
-- ※滞留在庫ダッシュボードの除外条件は適用しない
--   理由: 未来案件に紐づく在庫は「Preparing」や「LendingToB」ステータスのため
stocks AS (
  SELECT
    s.id,
    s.status,
    s.part_id,
    s.supplier_id,
    s.impairment_date,
    s.cost
  FROM `clas-analytics.lake.stock` s
  WHERE s.deleted_at IS NULL
),

-- ============================================================================
-- 3. 滞留開始日の計算
-- ============================================================================
-- 3-1. 入庫日
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    COALESCE(MAX(fa.arrival_at), ANY_VALUE(s.arrival_at)) AS arrival_at
  FROM `clas-analytics.lake.stock` s
  LEFT JOIN `clas-analytics.finance.fixed_asset_register` fa ON s.id = fa.stock_id
  WHERE s.deleted_at IS NULL
  GROUP BY s.id, s.part_id
),

-- 3-2. 初回EC出荷日
first_ec_ship AS (
  SELECT
    detail.stock_id,
    MIN(lent.shipped_at) AS first_shipped_at
  FROM `clas-analytics.lake.lent` lent
  INNER JOIN `clas-analytics.lake.lent_detail` detail ON lent.id = detail.lent_id
  WHERE lent.status NOT IN ('Cancel', 'PurchaseFailed')
    AND lent.shipped_at IS NOT NULL
    AND lent.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  GROUP BY detail.stock_id
),

-- 3-3. 初回法人出荷日
first_b2b_ship AS (
  SELECT
    detail.stock_id,
    MIN(lent_b.start_at) AS first_start_at
  FROM `clas-analytics.lake.lent_to_b` lent_b
  INNER JOIN `clas-analytics.lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status NOT IN ('Cancel') AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL
  GROUP BY detail.stock_id
),

-- 3-4. 貸出履歴（返却日）
all_lent_history AS (
  SELECT detail.stock_id, lent_b.start_at AS event_date, 'B2B_Ship' AS event_type
  FROM `clas-analytics.lake.lent_to_b` lent_b
  INNER JOIN `clas-analytics.lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status NOT IN ('Cancel') AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  UNION ALL
  SELECT detail.stock_id, lent.return_at AS event_date, 'EC_Return' AS event_type
  FROM `clas-analytics.lake.lent` lent
  INNER JOIN `clas-analytics.lake.lent_detail` detail ON lent.id = detail.lent_id
  WHERE lent.status = 'Returned' AND lent.return_at IS NOT NULL AND lent.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
  UNION ALL
  SELECT detail.stock_id, lent_b.end_at AS event_date, 'B2B_Return' AS event_type
  FROM `clas-analytics.lake.lent_to_b` lent_b
  INNER JOIN `clas-analytics.lake.lent_to_b_detail` detail ON lent_b.id = detail.lent_to_b_id
  WHERE lent_b.status = 'Ended' AND lent_b.end_at IS NOT NULL AND lent_b.deleted_at IS NULL AND detail.deleted_at IS NULL AND detail.stock_id IS NOT NULL
),

-- 3-5. 滞留開始日の特定（全在庫対象）
-- 滞留開始日 = 入庫日または最終出荷日のうち、より新しい方
stock_retention_start AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    s.impairment_date,
    s.status,
    -- 滞留開始日: 入庫日と最終出荷日の大きい方
    GREATEST(
      COALESCE(CAST(sa.arrival_at AS DATE), DATE('1900-01-01')),
      COALESCE(first_ec.first_shipped_at, DATE('1900-01-01')),
      COALESCE(first_b2b.first_start_at, DATE('1900-01-01'))
    ) AS retention_start_date
  FROM stocks s
  LEFT JOIN stock_arrivals sa ON s.id = sa.stock_id
  LEFT JOIN first_ec_ship first_ec ON s.id = first_ec.stock_id
  LEFT JOIN first_b2b_ship first_b2b ON s.id = first_b2b.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND sa.arrival_at < DATE('9999-01-01')  -- 仮日付除外
),

-- ============================================================================
-- 4. 2026年2月末減損対象の在庫を特定
-- ============================================================================
term1_stocks AS (
  SELECT
    srs.stock_id,
    srs.part_id,
    srs.status AS stock_status,
    srs.retention_start_date,
    DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) AS impairment_due_date
  FROM stock_retention_start srs
  CROSS JOIN target_term tt
  WHERE srs.impairment_date IS NULL  -- 減損済みは除外
    AND DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= tt.term_end  -- 2026年2月末までに365日経過
    AND DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) > CURRENT_DATE()  -- まだ減損していない
),

-- ============================================================================
-- 5. 未来の法人案件に紐づく在庫
-- ============================================================================
future_b2b AS (
  SELECT
    ltd.stock_id,
    'toB' AS order_type,
    c.id AS contract_id,
    ltb.shipping_date AS future_date,
    p.name AS proposition_name,
    ltb.status AS order_status
  FROM `clas-analytics.lake.lent_to_b_detail` ltd
  INNER JOIN `clas-analytics.lake.lent_to_b` ltb ON ltd.lent_to_b_id = ltb.id
  INNER JOIN `clas-analytics.lake.contract_destination` cd ON ltb.contract_destination_id = cd.id
  INNER JOIN `clas-analytics.lake.contract` c ON cd.contract_id = c.id
  INNER JOIN `clas-analytics.lake.proposition` p ON c.proposition_id = p.id
  CROSS JOIN target_term tt
  WHERE ltb.shipping_date > tt.term_end  -- 2026年2月末より後
    AND ltb.shipping_date < DATE('9999-01-01')  -- 仮日付除外
    AND ltb.deleted_at IS NULL
    AND ltd.deleted_at IS NULL
),

-- ============================================================================
-- 6. 未来のEC注文に紐づく在庫
-- ============================================================================
-- ECの場合はlent_idを契約IDとして扱い、顧客名をプロポジション名に相当するものとして出力
future_ec AS (
  SELECT
    ld.stock_id,
    'toC' AS order_type,
    l.id AS lent_id,
    l.shipped_at AS future_date,
    l.name AS customer_name,
    l.status AS order_status
  FROM `clas-analytics.lake.lent_detail` ld
  INNER JOIN `clas-analytics.lake.lent` l ON ld.lent_id = l.id
  CROSS JOIN target_term tt
  WHERE l.shipped_at > tt.term_end  -- 2026年2月末より後
    AND l.status IN ('Preparing', 'Adjusting', 'Lending')
    AND l.deleted_at IS NULL
    AND ld.deleted_at IS NULL
),

-- ============================================================================
-- 7. 未来案件の統合
-- ============================================================================
future_orders AS (
  SELECT
    stock_id,
    order_type,
    contract_id,
    future_date,
    proposition_name,
    order_status
  FROM future_b2b
  UNION ALL
  SELECT
    stock_id,
    order_type,
    lent_id AS contract_id,
    future_date,
    customer_name AS proposition_name,
    order_status
  FROM future_ec
),

-- ============================================================================
-- 8. 簿価情報（2026年2月末時点）
-- ============================================================================
book_values AS (
  SELECT
    stock_id,
    book_value_closing
  FROM `clas-analytics.mart.monthly_stock_valuation`
  WHERE period_end = DATE('2026-02-28')
),

-- ============================================================================
-- 9. 在庫マスタ情報
-- ============================================================================
stock_master AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    p.name AS part_name,
    sup.name AS supplier_name,
    s.cost AS actual_cost
  FROM `clas-analytics.lake.stock` s
  INNER JOIN `clas-analytics.lake.part` p ON s.part_id = p.id
  LEFT JOIN `clas-analytics.lake.supplier` sup ON s.supplier_id = sup.id
  WHERE s.deleted_at IS NULL
)

-- ============================================================================
-- 最終出力
-- ============================================================================
SELECT
  -- 識別情報
  t.stock_id AS `在庫ID`,
  t.stock_status AS `在庫ステータス`,
  sm.part_id AS `パーツID`,
  sm.part_name AS `パーツ名`,
  sm.supplier_name AS `サプライヤー`,

  -- 簿価情報（2026年2月末時点）
  sm.actual_cost AS `取得原価`,
  IFNULL(bv.book_value_closing, 0) AS `簿価_2026年2月末`,

  -- 滞留情報
  t.retention_start_date AS `滞留開始日`,
  tt.term_end AS `減損予定日`,  -- 対象在庫は2月28日で統一

  -- 未来案件情報
  fo.order_type AS `案件種別`,
  fo.contract_id AS `契約ID`,
  fo.proposition_name AS `案件名`,
  fo.order_status AS `案件ステータス`,
  fo.future_date AS `配送予定日`

FROM term1_stocks t
CROSS JOIN target_term tt
INNER JOIN future_orders fo ON t.stock_id = fo.stock_id
INNER JOIN stock_master sm ON t.stock_id = sm.stock_id
LEFT JOIN book_values bv ON t.stock_id = bv.stock_id

ORDER BY fo.order_type, fo.future_date, t.stock_id
;
