-- ============================================================================
-- 固定資産台帳 月次簿価計算クエリ（SELECT版）
-- ============================================================================
-- 概要:
--   在庫ごとに月次の簿価情報を計算するクエリ。
--   Looker Studioなどで任意の期間を指定して簿価集計を行うための
--   ハイブリッド設計（月次増減 + 累計値）を採用。
--
-- 対象期間: 2025年3月〜2027年2月（24ヶ月分）
-- 作成日: 2025-12-26
--
-- 出力カラム:
--   - 期首/期末の計算基準日
--   - 在庫の識別情報・マスタ情報
--   - 期首/増加/減少/期末の取得原価・減価償却累計額・減損損失累計額・簿価
--   - 累計値（任意期間計算用、2025年3月1日からの累積）
--
-- 使用方法:
--   1. 月単位での確認: そのまま実行
--   2. 任意期間での集計: 累計値の差分（期末 - 期首前月末）でフロー値を算出
-- ============================================================================

-- ============================================================================
-- 一時関数: 基本ユーティリティ
-- ============================================================================

-- 2日付間の月数を計算
CREATE TEMP FUNCTION calc_months_between(from_date DATE, to_date DATE) AS (
  12 * (EXTRACT(YEAR FROM to_date) - EXTRACT(YEAR FROM from_date))
  + EXTRACT(MONTH FROM to_date) - EXTRACT(MONTH FROM from_date)
);

-- 契約開始コード（YYMM形式）から日付を生成
-- 例: '2207' → 2022-07-01
CREATE TEMP FUNCTION parse_contract_date(code STRING) AS (
  CASE
    WHEN code IS NULL OR LENGTH(code) != 4 THEN NULL
    ELSE DATE(
      CAST(CONCAT('20', SUBSTR(code, 1, 2)) AS INT64),
      CAST(SUBSTR(code, 3, 2) AS INT64),
      1
    )
  END
);

-- ============================================================================
-- 一時関数: サプライヤーパターン判定
-- ============================================================================

-- 株式会社カンム リースバック品パターン
-- 例: '株式会社カンム 契約No.20220001'
CREATE TEMP FUNCTION is_kanmu_pattern(supplier_name STRING) AS (
  REGEXP_CONTAINS(supplier_name, r'^株式会社カンム 契約No\.2022000(\d)$')
);

-- 三井住友トラスト・パナソニックファイナンス リースバック品パターン
-- 例: '三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始2207'
CREATE TEMP FUNCTION is_smtpf_pattern(supplier_name STRING, contract_code STRING) AS (
  STARTS_WITH(supplier_name, '三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始')
  AND contract_code IS NOT NULL AND LENGTH(contract_code) = 4
);

-- レベシェア品・小物等・サンプル品（簿価計算対象外）の判定
-- 対象: レベシェア品、法人小物管理用、サンプル品
CREATE TEMP FUNCTION is_reveshare(supplier_name STRING, sample BOOL) AS (
  (REGEXP_CONTAINS(supplier_name, 'レベシェア')
   AND NOT REGEXP_CONTAINS(supplier_name, 'リース・レベシェア'))
  OR REGEXP_CONTAINS(supplier_name, '法人小物管理用')
  OR sample
);

-- ============================================================================
-- 一時関数: 資産分類・会計ステータス
-- ============================================================================

-- 資産分類の判定
CREATE TEMP FUNCTION classify_asset(supplier_name STRING, sample BOOL) AS (
  CASE
    WHEN REGEXP_CONTAINS(supplier_name, 'リース・レベシェア') THEN 'リース資産(借手リース)'
    WHEN REGEXP_CONTAINS(supplier_name, 'レベシェア') THEN 'レベシェア品'
    WHEN REGEXP_CONTAINS(supplier_name, '法人小物管理用') THEN '小物等'
    WHEN sample THEN 'サンプル品'
    ELSE '賃貸用固定資産'
  END
);

-- 破損紛失分類の英語→日本語変換
CREATE TEMP FUNCTION convert_impossibility_to_ja(classification STRING) AS (
  CASE classification
    WHEN 'BadDebt' THEN '貸倒'
    WHEN 'CustomerLost' THEN '顧客・配送業者による紛失'
    WHEN 'InitialDefect' THEN '初期不良'
    WHEN 'InspectionUnnecessaryDisposal' THEN '検品不要廃棄'
    WHEN 'Malfunction' THEN '動作不良'
    WHEN 'PartProcurementNotPossible' THEN '部品・消耗品調達不可'
    WHEN 'PartUnification' THEN 'パーツ統合'
    WHEN 'PlannedDisposal' THEN '計画廃棄'
    WHEN 'ProductDefectDamage' THEN '商品不良（破損）'
    WHEN 'ProductDefectDent' THEN '商品不良（へこみ）'
    WHEN 'ProductDefectDirt' THEN '商品不良（汚れ）'
    WHEN 'ProductDefectMold' THEN '商品不良（カビ）'
    WHEN 'ProductDefectScratch' THEN '商品不良（キズ）'
    WHEN 'ProductDefectSmell' THEN '商品不良（におい）'
    WHEN 'SampleDisposal' THEN 'サンプルの廃棄'
    WHEN 'SoldOn30days' THEN '売却（30日）'
    WHEN 'SoldToBusiness' THEN '売却（法人案件）'
    WHEN 'SoldToCustomer' THEN '売却（顧客）'
    WHEN 'SoldToEcommerce' THEN '売却（EC）'
    WHEN 'SoldToExternalSale' THEN '売却（外部販売）'
    WHEN 'SoldToRecycler' THEN '売却（処分）'
    WHEN 'WarehouseLost' THEN '庫内紛失／棚卸差異'
    ELSE classification
  END
);

-- 会計ステータスの判定（期末時点の状態に基づく）
CREATE TEMP FUNCTION calc_accounting_status(
  asset_class STRING,
  inspected_at DATE,
  impossibled_at DATE,
  lease_start_at DATE,
  classification_of_impossibility STRING,
  period_end DATE
) AS (
  CASE
    WHEN asset_class = 'レベシェア品' THEN '計上外(レベシェア)'
    WHEN asset_class = '小物等' THEN '仕入高(小物等)'
    WHEN asset_class = 'サンプル品' THEN '研究開発費(サンプル品)'
    WHEN inspected_at > period_end OR inspected_at IS NULL THEN '計上外(入庫検品前)'
    WHEN impossibled_at <= period_end
         AND classification_of_impossibility IN ('SoldToCustomer', 'SoldToBusiness', 'SoldToEcommerce')
    THEN '仕入高(売却)'
    WHEN impossibled_at <= period_end
         AND classification_of_impossibility = 'BadDebt'
    THEN '雑費(除却)'
    WHEN impossibled_at <= period_end THEN '家具廃棄損(除却)'
    WHEN lease_start_at <= period_end THEN 'リース債権(貸手リース)'
    ELSE asset_class
  END
);

-- ============================================================================
-- 一時関数: リースバック品の日付計算
-- ============================================================================

-- リース再取得日の計算
-- リースバック品が自社に戻ってくる日付を算出
CREATE TEMP FUNCTION calc_lease_reacquisition_date(supplier_name STRING, contract_code STRING) AS (
  CASE
    -- カンム: 契約番号から月を取得し、2024年の該当月1日
    WHEN is_kanmu_pattern(supplier_name)
    THEN PARSE_DATE('%Y-%m-%d',
      CONCAT('2024-', REGEXP_EXTRACT(supplier_name, r'契約No\.2022000(\d)'), '-1'))
    -- 三井住友トラスト: 契約開始から30ヶ月後の月末
    WHEN is_smtpf_pattern(supplier_name, contract_code)
    THEN DATE_SUB(
      DATE_TRUNC(DATE_ADD(parse_contract_date(contract_code), INTERVAL 31 MONTH), MONTH),
      INTERVAL 1 DAY
    )
    -- その他（_2510/_2511サフィックス）
    WHEN ENDS_WITH(supplier_name, '_2510') THEN DATE '2025-10-01'
    WHEN ENDS_WITH(supplier_name, '_2511') THEN DATE '2025-11-01'
    ELSE NULL
  END
);

-- 初回出荷日（供与開始日）の計算
-- リースバック品は契約に基づいた日付を使用
CREATE TEMP FUNCTION calc_first_shipped_at(
  supplier_name STRING,
  contract_code STRING,
  original_first_shipped_at DATE
) AS (
  CASE
    -- カンム: 契約番号から月を取得し、2022年の該当月1日
    WHEN is_kanmu_pattern(supplier_name)
    THEN PARSE_DATE('%Y-%m-%d',
      CONCAT('2022-', REGEXP_EXTRACT(supplier_name, r'契約No\.2022000(\d)'), '-1'))
    -- 三井住友トラスト: 契約開始月の月末
    WHEN is_smtpf_pattern(supplier_name, contract_code)
    THEN LAST_DAY(parse_contract_date(contract_code))
    -- その他
    WHEN ENDS_WITH(supplier_name, '_2510') THEN DATE '2025-10-01'
    WHEN ENDS_WITH(supplier_name, '_2511') THEN DATE '2025-11-01'
    ELSE original_first_shipped_at
  END
);

-- 貸手リース開始日の調整
-- リース再取得日より前のリース開始日はNULLに（カンムは例外として元の値を保持）
CREATE TEMP FUNCTION adjust_lease_start_at(
  supplier_name STRING,
  contract_code STRING,
  lease_start_at_raw DATE
) AS (
  CASE
    WHEN lease_start_at_raw IS NULL THEN NULL
    -- カンムは常に元の値を保持
    WHEN is_kanmu_pattern(supplier_name) THEN lease_start_at_raw
    -- 三井住友トラスト: リース再取得日より前ならNULL
    WHEN is_smtpf_pattern(supplier_name, contract_code)
         AND lease_start_at_raw < DATE_SUB(
           DATE_TRUNC(DATE_ADD(parse_contract_date(contract_code), INTERVAL 31 MONTH), MONTH),
           INTERVAL 1 DAY)
    THEN NULL
    -- その他
    WHEN ENDS_WITH(supplier_name, '_2510') AND lease_start_at_raw < DATE '2025-10-01'
    THEN NULL
    WHEN ENDS_WITH(supplier_name, '_2511') AND lease_start_at_raw < DATE '2025-11-01'
    THEN NULL
    ELSE lease_start_at_raw
  END
);

-- ============================================================================
-- 一時関数: 償却月数計算
-- ============================================================================

-- 指定日時点での償却済み月数を計算
--
-- 引数:
--   reference_date: 判定基準日（期首または期末）
--   is_inclusive: TRUE=基準日を含む(<=)、FALSE=含まない(<)
--
-- 償却停止の優先順位:
--   1. 減損日（除却・リースより前に発生した場合）
--   2. リース開始日
--   3. 除売却日（庫内紛失は当月まで、その他は前月まで）
--   4. 通常（継続償却中）
CREATE TEMP FUNCTION calc_depreciation_months_at_point(
  first_shipped_at DATE,
  impairment_date DATE,
  lease_start_at DATE,
  impossibled_at DATE,
  classification_of_impossibility STRING,
  reference_date DATE,
  is_inclusive BOOL
) AS (
  CASE
    -- 初回出荷前または出荷日なし
    WHEN first_shipped_at IS NULL THEN 0
    WHEN NOT is_inclusive AND first_shipped_at >= reference_date THEN 0
    WHEN is_inclusive AND first_shipped_at > reference_date THEN 0
    -- 減損日が最優先
    WHEN (NOT is_inclusive AND impairment_date < reference_date
          OR is_inclusive AND impairment_date <= reference_date)
         AND (impossibled_at IS NULL OR impossibled_at > impairment_date)
         AND (lease_start_at IS NULL OR lease_start_at > impairment_date)
    THEN GREATEST(calc_months_between(first_shipped_at, impairment_date) + 1, 0)
    -- リース開始（開始月の前月まで償却）
    WHEN (NOT is_inclusive AND lease_start_at < reference_date
          OR is_inclusive AND lease_start_at <= reference_date)
    THEN GREATEST(calc_months_between(first_shipped_at, lease_start_at), 0)
    -- 庫内紛失（当月まで償却）
    WHEN (NOT is_inclusive AND impossibled_at < reference_date
          OR is_inclusive AND impossibled_at <= reference_date)
         AND classification_of_impossibility = 'WarehouseLost'
    THEN GREATEST(calc_months_between(first_shipped_at, impossibled_at) + 1, 0)
    -- その他の除売却（前月まで償却）
    WHEN (NOT is_inclusive AND impossibled_at < reference_date
          OR is_inclusive AND impossibled_at <= reference_date)
    THEN GREATEST(calc_months_between(first_shipped_at, impossibled_at), 0)
    -- 通常（継続償却中）
    WHEN is_inclusive
    THEN calc_months_between(first_shipped_at, reference_date) + 1
    ELSE calc_months_between(first_shipped_at, reference_date)
  END
);

-- ============================================================================
-- 一時関数: 簿価計算
-- ============================================================================

-- 簿価 = 取得原価 - 減価償却累計額 - 減損損失累計額
-- レベシェア品は常に0を返す
CREATE TEMP FUNCTION calc_book_value(
  acquisition_cost FLOAT64,
  accum_depreciation FLOAT64,
  impairment FLOAT64,
  is_reveshare BOOL
) AS (
  CASE WHEN is_reveshare THEN 0.0
       ELSE acquisition_cost - accum_depreciation - impairment
  END
);

-- ============================================================================
-- CTE 0: 月次期間マスタ
-- 2025年3月〜2027年2月の24ヶ月分を生成
-- ============================================================================
WITH monthly_periods AS (
  SELECT
    DATE_TRUNC(month_date, MONTH) AS period_start,
    LAST_DAY(month_date) AS period_end
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2025-03-01',
    DATE '2027-02-01',
    INTERVAL 1 MONTH
  )) AS month_date
),

-- ============================================================================
-- CTE 1: 基礎データ取得
-- 在庫・パーツ・サプライヤー・取得原価・固定資産台帳を結合
-- ============================================================================
base_stock_data AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    p.name AS part_name,
    sup.name AS supplier_name,
    s.sample,
    s.classification_of_impossibility,
    s.inspected_at,
    s.impossibled_at,
    s.impairment_date,
    far.lease_start_at AS lease_start_at_raw,
    far.first_shipped_at AS first_shipped_at_base,
    COALESCE(far.depreciation_period, 0) AS depreciation_period,
    -- 取得原価 = 仕入原価 + 諸経費 - 値引
    COALESCE(s.cost, 0) + COALESCE(sac.overhead_cost, 0) - COALESCE(sac.discount, 0) AS actual_cost,
    -- 月次償却額 = 取得原価 / (耐用年数 × 12) の切り上げ
    CASE
      WHEN COALESCE(far.depreciation_period, 0) = 0 THEN 0
      ELSE CEILING(
        (COALESCE(s.cost, 0) + COALESCE(sac.overhead_cost, 0) - COALESCE(sac.discount, 0))
        / (far.depreciation_period * 12)
      )
    END AS monthly_depreciation,
    -- サプライヤー名から契約開始コードを抽出
    REGEXP_EXTRACT(sup.name, r'_契約開始(\d{4})$') AS contract_code,
    si.sold_proposition_name,
    si.lease_proposition_name

  FROM `clas-analytics.lake.stock` s
  LEFT JOIN `clas-analytics.lake.part` p ON s.part_id = p.id
  LEFT JOIN `clas-analytics.lake.supplier` sup ON s.supplier_id = sup.id
  LEFT JOIN (
    -- 取得原価の諸経費・値引を集計
    SELECT stock_id,
           SUM(COALESCE(overhead_cost, 0)) AS overhead_cost,
           SUM(COALESCE(discount, 0)) AS discount
    FROM `clas-analytics.lake.stock_acquisition_costs`
    GROUP BY stock_id
  ) sac ON s.id = sac.stock_id
  LEFT JOIN (
    -- 固定資産台帳から最新期の情報を取得
    SELECT stock_id,
           ANY_VALUE(depreciation_period) AS depreciation_period,
           ANY_VALUE(first_shipped_at) AS first_shipped_at,
           ANY_VALUE(lease_start_at) AS lease_start_at
    FROM `clas-analytics.finance.fixed_asset_register`
    WHERE term = (SELECT MAX(term) FROM `clas-analytics.finance.fixed_asset_register`)
    GROUP BY stock_id
  ) far ON s.id = far.stock_id
  LEFT JOIN `clas-analytics.mart.stock_info` si ON s.id = si.stock_id
  WHERE s.deleted_at IS NULL
),

-- ============================================================================
-- CTE 2: 在庫×月次のクロス結合 + 計算済みカラム
-- ============================================================================
stock_monthly_base AS (
  SELECT
    mp.period_start,
    mp.period_end,
    bsd.*,
    -- リースバック関連の日付計算
    calc_lease_reacquisition_date(bsd.supplier_name, bsd.contract_code) AS lease_reacquisition_date,
    calc_first_shipped_at(bsd.supplier_name, bsd.contract_code, bsd.first_shipped_at_base) AS first_shipped_at,
    adjust_lease_start_at(bsd.supplier_name, bsd.contract_code, bsd.lease_start_at_raw) AS lease_start_at,
    -- フラグ
    is_reveshare(bsd.supplier_name, bsd.sample) AS is_reveshare_flag,
    -- 耐用月数
    bsd.depreciation_period * 12 AS max_months
  FROM base_stock_data bsd
  CROSS JOIN monthly_periods mp
),

-- ============================================================================
-- CTE 3: 派生カラム追加
-- ============================================================================
stock_monthly_enriched AS (
  SELECT
    *,
    -- リース再取得日の月初（比較用）
    DATE_TRUNC(lease_reacquisition_date, MONTH) AS reacq_month
  FROM stock_monthly_base
),

-- ============================================================================
-- CTE 4: 償却月数計算
-- α: 期首時点の償却済み月数
-- β: 期末時点の償却済み月数
-- γ: リース再取得時点の償却済み月数
-- ============================================================================
with_depreciation_months AS (
  SELECT
    *,
    calc_depreciation_months_at_point(
      first_shipped_at, impairment_date, lease_start_at, impossibled_at,
      classification_of_impossibility, period_start, FALSE
    ) AS shokyaku_alpha,
    calc_depreciation_months_at_point(
      first_shipped_at, impairment_date, lease_start_at, impossibled_at,
      classification_of_impossibility, period_end, TRUE
    ) AS shokyaku_beta,
    CASE
      WHEN lease_reacquisition_date IS NULL THEN 0
      ELSE calc_depreciation_months_at_point(
        first_shipped_at, impairment_date, lease_start_at, impossibled_at,
        classification_of_impossibility, DATE_TRUNC(lease_reacquisition_date, MONTH), FALSE
      )
    END AS shokyaku_gamma
  FROM stock_monthly_enriched
),

-- ============================================================================
-- CTE 5: 取得原価・償却月数計算
-- 期首/増加/減少/期末の4時点での値を計算
-- ============================================================================
with_costs_and_amort AS (
  SELECT
    *,
    -- 期首取得原価
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN inspected_at IS NULL OR period_start <= inspected_at THEN 0
      WHEN period_start <= reacq_month THEN 0
      WHEN impossibled_at < period_start OR lease_start_at < period_start THEN 0
      ELSE actual_cost
    END AS acquisition_cost_opening,
    -- 増加取得原価（当月入庫またはリース再取得）
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN period_start <= lease_reacquisition_date AND lease_reacquisition_date <= period_end
           AND (impossibled_at IS NULL OR impossibled_at >= reacq_month)
           AND (lease_start_at IS NULL OR lease_start_at >= reacq_month)
      THEN actual_cost
      WHEN period_start <= inspected_at AND inspected_at <= period_end
      THEN actual_cost
      ELSE 0
    END AS acquisition_cost_increase,
    -- 減少取得原価（当月除売却またはリース開始）
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN impossibled_at < period_start OR lease_start_at < period_start THEN 0
      WHEN impossibled_at < reacq_month OR lease_start_at < reacq_month THEN 0
      WHEN lease_start_at > period_end THEN 0
      WHEN lease_start_at IS NULL AND (impossibled_at IS NULL OR impossibled_at > period_end) THEN 0
      ELSE actual_cost
    END AS acquisition_cost_decrease,
    -- 期末取得原価
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN inspected_at IS NULL THEN 0
      WHEN reacq_month > period_end THEN 0
      WHEN inspected_at > period_end OR impossibled_at <= period_end OR lease_start_at <= period_end THEN 0
      ELSE actual_cost
    END AS acquisition_cost_closing,
    -- 期首償却月数
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN inspected_at >= period_start THEN 0
      WHEN lease_start_at < period_start OR impossibled_at < period_start THEN 0
      ELSE LEAST(shokyaku_alpha, max_months)
    END AS amort_months_opening,
    -- 当月償却月数
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN inspected_at > period_end OR reacq_month > period_end THEN 0
      WHEN reacq_month >= period_start
      THEN LEAST(max_months, shokyaku_beta) - LEAST(max_months, shokyaku_gamma)
      ELSE LEAST(max_months, shokyaku_beta) - LEAST(max_months, shokyaku_alpha)
    END AS amort_months_depreciation,
    -- 増加償却月数
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN reacq_month < period_start OR reacq_month > period_end
           OR impossibled_at < reacq_month OR lease_start_at < reacq_month THEN 0
      ELSE LEAST(shokyaku_gamma, max_months)
    END AS amort_months_increase,
    -- 減少償却月数
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN impossibled_at < period_start OR lease_start_at < period_start THEN 0
      WHEN impossibled_at < reacq_month OR lease_start_at < reacq_month THEN 0
      WHEN lease_start_at > period_end THEN 0
      WHEN lease_start_at IS NULL AND (impossibled_at IS NULL OR impossibled_at > period_end) THEN 0
      ELSE LEAST(shokyaku_beta, max_months)
    END AS amort_months_decrease,
    -- 期末償却月数
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN inspected_at > period_end THEN 0
      WHEN lease_start_at <= period_end OR impossibled_at <= period_end THEN 0
      ELSE LEAST(shokyaku_beta, max_months)
    END AS amort_months_closing
  FROM with_depreciation_months
),

-- ============================================================================
-- CTE 6: 減価償却累計額計算
-- ============================================================================
with_accum_depreciation AS (
  SELECT
    *,
    -- 期首減価償却累計額
    CASE
      WHEN is_reveshare_flag OR amort_months_opening = 0 THEN 0
      WHEN period_start <= reacq_month THEN 0
      WHEN actual_cost < LEAST(max_months, shokyaku_alpha) OR max_months <= shokyaku_alpha
      THEN actual_cost
      ELSE monthly_depreciation * amort_months_opening
    END AS accum_depr_opening,
    -- 増加減価償却累計額
    CASE
      WHEN is_reveshare_flag OR amort_months_increase = 0 THEN 0
      WHEN actual_cost < LEAST(max_months, shokyaku_gamma) OR max_months = amort_months_increase
      THEN actual_cost
      ELSE monthly_depreciation * amort_months_increase
    END AS accum_depr_increase,
    -- 減少減価償却累計額
    CASE
      WHEN is_reveshare_flag OR amort_months_decrease = 0 THEN 0
      WHEN actual_cost < LEAST(max_months, shokyaku_beta) OR max_months = amort_months_decrease
      THEN actual_cost
      ELSE monthly_depreciation * amort_months_decrease
    END AS accum_depr_decrease,
    -- 期中減価償却費
    CASE
      WHEN is_reveshare_flag OR amort_months_depreciation = 0 THEN 0
      WHEN actual_cost <= max_months AND actual_cost > shokyaku_alpha AND actual_cost < shokyaku_beta
      THEN actual_cost - shokyaku_alpha
      WHEN actual_cost < LEAST(shokyaku_alpha, max_months) THEN 0
      WHEN max_months <= shokyaku_beta
      THEN actual_cost - monthly_depreciation * (max_months - 1)
           + monthly_depreciation * (amort_months_depreciation - 1)
      ELSE monthly_depreciation * amort_months_depreciation
    END AS interim_depr_expense,
    -- 期末減価償却累計額
    CASE
      WHEN is_reveshare_flag OR amort_months_closing = 0 THEN 0
      WHEN reacq_month > period_end THEN 0
      WHEN actual_cost < LEAST(max_months, shokyaku_beta) OR max_months <= shokyaku_beta
      THEN actual_cost
      ELSE monthly_depreciation * amort_months_closing
    END AS accum_depr_closing
  FROM with_costs_and_amort
),

-- ============================================================================
-- CTE 7: 減損損失計算
-- ============================================================================
with_impairment AS (
  SELECT
    *,
    -- 期首減損損失累計額
    CASE
      WHEN is_reveshare_flag THEN 0
      WHEN impairment_date < period_start
      THEN acquisition_cost_opening - accum_depr_opening
      ELSE 0
    END AS impairment_opening,
    -- 増加減損損失累計額（当期減損損失と同値）
    CASE
      WHEN is_reveshare_flag OR reacq_month > period_end THEN 0
      WHEN inspected_at <= period_end AND period_start <= impairment_date AND impairment_date <= period_end
      THEN (acquisition_cost_opening - accum_depr_opening)
           + (acquisition_cost_increase - accum_depr_increase)
           - interim_depr_expense
      ELSE 0
    END AS impairment_increase,
    -- 減少減損損失累計額
    CASE
      WHEN is_reveshare_flag OR impairment_date > period_end OR impairment_date IS NULL THEN 0
      ELSE acquisition_cost_decrease - accum_depr_decrease
    END AS impairment_decrease,
    -- 期末減損損失累計額
    CASE
      WHEN is_reveshare_flag OR reacq_month > period_end THEN 0
      WHEN impairment_date <= period_end
      THEN acquisition_cost_closing - accum_depr_closing
      ELSE 0
    END AS impairment_closing
  FROM with_accum_depreciation
),

-- ============================================================================
-- CTE 8: 累計値計算
-- 任意期間での集計用に、2025年3月からの累計値を計算
-- 使用方法: 期末累計 - 期首前月末累計 = 期間内のフロー値
-- ============================================================================
with_cumulative AS (
  SELECT
    *,
    SUM(acquisition_cost_increase) OVER w AS cumulative_acquisition_cost_increase,
    SUM(acquisition_cost_decrease) OVER w AS cumulative_acquisition_cost_decrease,
    SUM(accum_depr_increase) OVER w AS cumulative_accum_depr_increase,
    SUM(accum_depr_decrease) OVER w AS cumulative_accum_depr_decrease,
    SUM(impairment_increase) OVER w AS cumulative_impairment_increase,
    SUM(impairment_decrease) OVER w AS cumulative_impairment_decrease,
    SUM(interim_depr_expense) OVER w AS cumulative_depr_expense,
    SUM(impairment_increase) OVER w AS cumulative_impairment
  FROM with_impairment
  WINDOW w AS (PARTITION BY stock_id ORDER BY period_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)

-- ============================================================================
-- 最終出力
-- ============================================================================
SELECT
  -- 期間情報 (Period)
  period_start,
  period_end,
  -- 識別情報 (Identification)
  stock_id,
  part_id,
  part_name,
  depreciation_period,
  supplier_name,
  actual_cost,
  -- 資産分類・会計ステータス (Asset Classification / Accounting Status)
  classify_asset(supplier_name, sample) AS asset_classification,
  calc_accounting_status(
    classify_asset(supplier_name, sample),
    inspected_at,
    impossibled_at,
    lease_start_at,
    classification_of_impossibility,
    period_end
  ) AS accounting_status,
  -- 日付・分類 (Dates / Classification)
  inspected_at,
  first_shipped_at,
  impairment_date,
  impossibled_at,
  convert_impossibility_to_ja(classification_of_impossibility) AS impossibility_classification_ja,
  sold_proposition_name,
  lease_start_at,
  lease_proposition_name,
  lease_reacquisition_date,
  monthly_depreciation,
  -- 期首（4項目）(Opening Balance)
  acquisition_cost_opening,
  accum_depr_opening,
  impairment_opening,
  calc_book_value(acquisition_cost_opening, accum_depr_opening, impairment_opening, is_reveshare_flag) AS book_value_opening,
  -- 増加（4項目）(Increase)
  acquisition_cost_increase,
  accum_depr_increase,
  impairment_increase,
  calc_book_value(acquisition_cost_increase, accum_depr_increase, impairment_increase, is_reveshare_flag) AS book_value_increase,
  -- 減少（4項目）(Decrease)
  acquisition_cost_decrease,
  accum_depr_decrease,
  impairment_decrease,
  calc_book_value(acquisition_cost_decrease, accum_depr_decrease, impairment_decrease, is_reveshare_flag) AS book_value_decrease,
  -- 期中（2項目）(Interim)
  interim_depr_expense,
  impairment_increase AS interim_impairment,
  -- 期末（4項目）(Closing Balance)
  acquisition_cost_closing,
  accum_depr_closing,
  impairment_closing,
  calc_book_value(acquisition_cost_closing, accum_depr_closing, impairment_closing, is_reveshare_flag) AS book_value_closing,
  -- 累計値（8項目）- 任意期間計算用 (Cumulative - for flexible period calculation)
  cumulative_acquisition_cost_increase,
  cumulative_acquisition_cost_decrease,
  cumulative_accum_depr_increase,
  cumulative_accum_depr_decrease,
  cumulative_impairment_increase,
  cumulative_impairment_decrease,
  cumulative_depr_expense,
  cumulative_impairment

FROM with_cumulative
ORDER BY stock_id ASC, period_start ASC;

