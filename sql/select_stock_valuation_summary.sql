-- ============================================================================
-- 簿価計算 期間集計クエリ
-- ============================================================================
-- 概要:
--   monthly_stock_valuationテーブルから任意期間の簿価情報を集計する。
--   stock_idごとに1行で出力し、期首・期末の値と期間内のフロー値を計算。
--
-- 使用方法:
--   1. 下記の期間設定を変更して任意期間を指定
--   2. BigQueryコンソールで実行、またはLooker Studioで使用
--
-- 等式の検証:
--   期首 + 期中増加 - 期中減少 - 期中償却 = 期末
--   この等式が成り立つようにフロー値は月次値のSUMで計算。
--
-- 作成日: 2025-12-26
-- ============================================================================

-- ============================================================================
-- 期間設定（ここを変更してください）
-- ============================================================================
WITH target_period AS (
  SELECT
    DATE('2025-03-01') AS start_date,  -- 期首日
    DATE('2025-11-30') AS end_date     -- 期末日
),
-- ============================================================================

-- 期首月・期末月のレコードを特定
period_bounds AS (
  SELECT
    stock_id,
    MIN(period_end) AS first_period_end,
    MAX(period_end) AS last_period_end
  FROM `clas-analytics.mart.monthly_stock_valuation`
  CROSS JOIN target_period tp
  WHERE period_end >= tp.start_date
    AND period_end <= tp.end_date
  GROUP BY stock_id
),

-- 期首月のデータ（期首値を取得）
opening_data AS (
  SELECT
    m.stock_id,
    m.period_start,
    m.acquisition_cost_opening,
    m.accum_depr_opening,
    m.impairment_opening,
    m.book_value_opening
  FROM `clas-analytics.mart.monthly_stock_valuation` m
  INNER JOIN period_bounds pb ON m.stock_id = pb.stock_id AND m.period_end = pb.first_period_end
),

-- 期末月のデータ（期末値とマスタ情報を取得）
closing_data AS (
  SELECT
    m.stock_id,
    m.period_end,
    m.part_id,
    m.part_name,
    m.depreciation_period,
    m.supplier_name,
    m.actual_cost,
    m.asset_classification,
    m.accounting_status,
    m.inspected_at,
    m.first_shipped_at,
    m.impairment_date,
    m.impossibled_at,
    m.impossibility_classification_ja,
    m.sold_proposition_name,
    m.lease_start_at,
    m.lease_proposition_name,
    m.lease_reacquisition_date,
    m.monthly_depreciation,
    m.acquisition_cost_closing,
    m.accum_depr_closing,
    m.impairment_closing,
    m.book_value_closing
  FROM `clas-analytics.mart.monthly_stock_valuation` m
  INNER JOIN period_bounds pb ON m.stock_id = pb.stock_id AND m.period_end = pb.last_period_end
),

-- 期間内の全月データを集計（フロー値）
flow_data AS (
  SELECT
    m.stock_id,
    -- 期中増加（4項目）- 月次値のSUM
    SUM(m.acquisition_cost_increase) AS acquisition_cost_increase_sum,
    SUM(m.accum_depr_increase) AS accum_depr_increase_sum,
    SUM(m.impairment_increase) AS impairment_increase_sum,
    SUM(m.book_value_increase) AS book_value_increase_sum,
    -- 期中減少（4項目）- 月次値のSUM
    SUM(m.acquisition_cost_decrease) AS acquisition_cost_decrease_sum,
    SUM(m.accum_depr_decrease) AS accum_depr_decrease_sum,
    SUM(m.impairment_decrease) AS impairment_decrease_sum,
    SUM(m.book_value_decrease) AS book_value_decrease_sum,
    -- 期中償却・減損（2項目）- 月次値のSUM
    SUM(m.interim_depr_expense) AS interim_depr_expense_sum,
    SUM(m.interim_impairment) AS interim_impairment_sum
  FROM `clas-analytics.mart.monthly_stock_valuation` m
  CROSS JOIN target_period tp
  WHERE m.period_end >= tp.start_date
    AND m.period_end <= tp.end_date
  GROUP BY m.stock_id
)

SELECT
  -- 期間情報
  o.period_start AS `期首日`,
  c.period_end AS `期末日`,

  -- 識別情報
  c.stock_id AS `在庫ID`,
  c.part_id AS `パーツID`,
  c.part_name AS `パーツ名`,
  c.depreciation_period AS `耐用年数`,
  c.supplier_name AS `サプライヤー名`,
  c.actual_cost AS `取得原価`,

  -- 資産分類・会計ステータス（期末時点）
  c.asset_classification AS `資産分類`,
  c.accounting_status AS `会計ステータス`,

  -- 日付・分類
  c.inspected_at AS `入庫検品完了日`,
  c.first_shipped_at AS `供与開始日`,
  c.impairment_date AS `減損日`,
  c.impossibled_at AS `除売却日`,
  c.impossibility_classification_ja AS `破損紛失分類`,
  c.sold_proposition_name AS `売却案件名`,
  c.lease_start_at AS `貸手リース開始日`,
  c.lease_proposition_name AS `貸手リース案件名`,
  c.lease_reacquisition_date AS `リース再取得日`,
  c.monthly_depreciation AS `月次償却額`,

  -- 期首（4項目）- 期首月のopeningを使用
  o.acquisition_cost_opening AS `期首取得原価`,
  o.accum_depr_opening AS `期首減価償却累計額`,
  o.impairment_opening AS `期首減損損失累計額`,
  o.book_value_opening AS `期首簿価`,

  -- 期中増加（4項目）- 月次値のSUM
  f.acquisition_cost_increase_sum AS `期中増加取得原価`,
  f.accum_depr_increase_sum AS `期中増加減価償却累計額`,
  f.impairment_increase_sum AS `期中増加減損損失累計額`,
  f.book_value_increase_sum AS `期中増加簿価`,

  -- 期中減少（4項目）- 月次値のSUM
  f.acquisition_cost_decrease_sum AS `期中減少取得原価`,
  f.accum_depr_decrease_sum AS `期中減少減価償却累計額`,
  f.impairment_decrease_sum AS `期中減少減損損失累計額`,
  f.book_value_decrease_sum AS `期中減少簿価`,

  -- 期中償却・減損（2項目）- 月次値のSUM
  f.interim_depr_expense_sum AS `期中減価償却費`,
  f.interim_impairment_sum AS `期中減損損失`,

  -- 期末（4項目）- 期末月のclosingを使用
  c.acquisition_cost_closing AS `期末取得原価`,
  c.accum_depr_closing AS `期末減価償却累計額`,
  c.impairment_closing AS `期末減損損失累計額`,
  c.book_value_closing AS `期末簿価`

FROM opening_data o
INNER JOIN closing_data c ON o.stock_id = c.stock_id
INNER JOIN flow_data f ON o.stock_id = f.stock_id
ORDER BY c.stock_id
;
