# 簿価計算SQL実装 最終計画書

**作成日**: 2025-12-26
**実装方式**: 方式B（事前計算テーブル）
**対象データ**: 全在庫 約20万件
**計算期間**: 実行日の前月末から翌年同月末まで（13ヶ月分）

---

## 📋 実装概要

### 目的
Looker Studioで実装されている簿価計算ロジックを、BigQuery SQLで完全移植し、月次の簿価推移を事前計算テーブルとして保持する。

### 採用方式
**方式B: 事前計算テーブル（固定値で全期間保存）**

**選定理由**:
1. 月次決算で確定値が必要（パラメータ間違いのリスク排除）
2. 月次推移分析が容易（Looker Studioでグラフ化）
3. 実行速度の向上（事前計算済みデータの参照）
4. 過去期間の簿価が即座に参照可能

---

## 📅 計算期間の定義

### 期間生成ロジック

**基準日**: クエリ実行日（例: 2025-12-26）

**期首日の計算**:
```sql
-- 実行日の会計期間の期首日（前の3月1日）
CASE
  WHEN EXTRACT(MONTH FROM CURRENT_DATE()) >= 3
  THEN DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 3, 1)  -- 今年の3月1日
  ELSE DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 3, 1)  -- 去年の3月1日
END
```

**計算対象期間**:
- 開始: 実行日の前月末（2025年11月30日）
- 終了: 実行日の翌年同月末（2026年11月30日）
- **期間数**: 13ヶ月分

**具体例（実行日が2025年12月26日の場合）**:

| 期間名 | 期首日 | 期末日 | 備考 |
|--------|--------|--------|------|
| 2025年11月 | 2025-03-01 | 2025-11-30 | 実行日の前月末 |
| 2025年12月 | 2025-03-01 | 2025-12-31 | 実行月 |
| 2026年1月 | 2025-03-01 | 2026-01-31 | |
| 2026年2月 | 2025-03-01 | 2026-02-28 | 会計年度末 |
| 2026年3月 | 2026-03-01 | 2026-03-31 | 新会計年度開始 |
| ... | ... | ... | |
| 2026年11月 | 2026-03-01 | 2026-11-30 | 実行日の翌年同月末 |

---

## 🏗️ データベース設計

### 1. 計算期間マスタテーブル

**テーブル名**: `clas-analytics.finance.book_value_calculation_periods`

**スキーマ**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.book_value_calculation_periods` AS
WITH period_params AS (
  SELECT
    -- 会計年度の期首日（前の3月1日）
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) >= 3
      THEN DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 3, 1)
      ELSE DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 3, 1)
    END AS fiscal_year_start,

    -- 計算開始月（実行日の前月）
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS calc_start,

    -- 計算終了月（実行日の翌年同月）
    DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH) AS calc_end
),
month_sequence AS (
  SELECT
    DATE_ADD(p.calc_start, INTERVAL offset MONTH) AS month_start
  FROM period_params p,
  UNNEST(GENERATE_ARRAY(0, 13)) AS offset  -- 0〜13で14ヶ月分（前月末〜翌年同月末）
)
SELECT
  FORMAT_DATE('%Y年%m月', LAST_DAY(m.month_start)) AS period_name,
  p.fiscal_year_start AS period_start,
  LAST_DAY(m.month_start) AS period_end,
  EXTRACT(YEAR FROM LAST_DAY(m.month_start)) AS year,
  EXTRACT(MONTH FROM LAST_DAY(m.month_start)) AS month
FROM month_sequence m
CROSS JOIN period_params p
ORDER BY period_end;
```

**カラム説明**:
| カラム名 | データ型 | 説明 | サンプル値 |
|---------|---------|------|-----------|
| `period_name` | STRING | 期間の表示名 | "2025年11月" |
| `period_start` | DATE | 会計年度の期首日 | 2025-03-01 |
| `period_end` | DATE | 計算対象月の月末日 | 2025-11-30 |
| `year` | INT64 | 年 | 2025 |
| `month` | INT64 | 月 | 11 |

---

### 2. 簿価計算結果テーブル

**テーブル名**: `clas-analytics.finance.book_value_calculation`

**スキーマ構造**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.book_value_calculation` AS
WITH ... (計算ロジック)
SELECT
  -- 識別情報
  stock_id INT64,
  period_name STRING,
  period_start DATE,
  period_end DATE,

  -- 基礎データ
  part_id INT64,
  part_name STRING,
  supplier_name STRING,
  actual_cost FLOAT64,  -- 実際の取得原価
  depreciation_period INT64,  -- 耐用年数
  monthly_depreciation FLOAT64,  -- 月次償却額

  -- 日付情報
  inspected_at DATE,  -- 入庫検品完了日
  first_shipped_at DATE,  -- 初回出荷日
  lease_reacquisition_date DATE,  -- リース再取得日
  impossibled_at DATE,  -- 除売却日
  impairment_date DATE,  -- 減損損失日
  lease_start_at DATE,  -- 貸手リース開始日

  -- フラグ・分類
  sample BOOLEAN,
  classification_of_impossibility STRING,

  -- 償却期間（α/β/γ）
  shokyaku_alpha INT64,  -- 償却α（期首時点償却済み月数）
  shokyaku_beta INT64,   -- 償却β（期末時点償却済み月数）
  shokyaku_gamma INT64,  -- 償却γ（リース再取得時点償却済み月数）

  -- 取得原価（4時点）
  acquisition_cost_opening FLOAT64,   -- 期首取得原価
  acquisition_cost_increase FLOAT64,  -- 増加取得原価
  acquisition_cost_decrease FLOAT64,  -- 減少取得原価
  acquisition_cost_closing FLOAT64,   -- 期末取得原価

  -- 償却月数（5時点）
  amortization_months_opening INT64,      -- 期首償却月数
  amortization_months_depreciation INT64, -- 償却償却月数（期中）
  amortization_months_increase INT64,     -- 増加償却月数
  amortization_months_decrease INT64,     -- 減少償却月数
  amortization_months_closing INT64,      -- 期末償却月数

  -- 減価償却累計額（5時点）
  accumulated_depreciation_opening FLOAT64,   -- 期首減価償却累計額
  accumulated_depreciation_increase FLOAT64,  -- 増加減価償却累計額
  accumulated_depreciation_decrease FLOAT64,  -- 減少減価償却累計額
  interim_depreciation_expense FLOAT64,       -- 期中減価償却費
  accumulated_depreciation_closing FLOAT64,   -- 期末減価償却累計額

  -- 減損損失累計額（5時点）
  impairment_loss_opening FLOAT64,   -- 期首減損損失累計額
  impairment_loss_increase FLOAT64,  -- 増加減損損失累計額
  impairment_loss_decrease FLOAT64,  -- 減少減損損失累計額
  interim_impairment_loss FLOAT64,   -- 期中減損損失
  impairment_loss_closing FLOAT64,   -- 期末減損損失累計額

  -- 簿価（4時点）★最重要
  book_value_opening FLOAT64,   -- 期首簿価
  book_value_increase FLOAT64,  -- 増加簿価
  book_value_decrease FLOAT64,  -- 減少簿価
  book_value_closing FLOAT64    -- 期末簿価
FROM ...
```

**データ量見積もり**:
- 在庫数: 20万件
- 期間数: 13ヶ月
- **総レコード数**: 20万件 × 13ヶ月 = **260万件**
- **カラム数**: 約40カラム
- **推定データサイズ**: 2〜3GB

---

## 🔧 SQL実装の構造

### CTE（Common Table Expression）の階層構造

```sql
WITH
-- レベル0: 基礎データの取得
base_stock_data AS (
  -- lake.stock, lake.part, lake.supplier,
  -- lake.stock_acquisition_costs, finance.fixed_asset_register を結合
  -- 実際の取得原価、月次償却額を計算
),

-- レベル0-1: 計算期間の生成
calculation_periods AS (
  SELECT * FROM `clas-analytics.finance.book_value_calculation_periods`
),

-- レベル0-2: 在庫 × 期間のクロス結合
stock_period_matrix AS (
  SELECT
    b.*,
    p.period_name,
    p.period_start,
    p.period_end
  FROM base_stock_data b
  CROSS JOIN calculation_periods p
),

-- レベル1: 契約識別コード、リース再取得日、初回出荷日の計算
enriched_data AS (
  SELECT
    *,
    REGEXP_EXTRACT(supplier_name, '_契約開始(\\d{4})$') AS contract_code,
    -- リース再取得日の計算（サプライヤー名から）
    CASE ... END AS lease_reacquisition_date,
    -- 初回出荷日の計算（サプライヤー名から）
    CASE ... END AS calculated_first_shipped_at
  FROM stock_period_matrix
),

-- レベル2: 償却α/β/γの計算
depreciation_periods AS (
  SELECT
    *,
    -- 償却α（期首時点償却済み月数）
    CASE ... END AS shokyaku_alpha,
    -- 償却β（期末時点償却済み月数）
    CASE ... END AS shokyaku_beta,
    -- 償却γ（リース再取得時点償却済み月数）
    CASE ... END AS shokyaku_gamma
  FROM enriched_data
),

-- レベル3: 取得原価（4時点）の計算
acquisition_costs AS (
  SELECT
    *,
    CASE ... END AS acquisition_cost_opening,   -- 期首取得原価
    CASE ... END AS acquisition_cost_increase,  -- 増加取得原価
    CASE ... END AS acquisition_cost_decrease,  -- 減少取得原価
    CASE ... END AS acquisition_cost_closing    -- 期末取得原価
  FROM depreciation_periods
),

-- レベル4: 償却月数（5時点）の計算
amortization_months AS (
  SELECT
    *,
    CASE ... END AS amortization_months_opening,      -- 期首償却月数
    CASE ... END AS amortization_months_depreciation, -- 償却償却月数
    CASE ... END AS amortization_months_increase,     -- 増加償却月数
    CASE ... END AS amortization_months_decrease,     -- 減少償却月数
    CASE ... END AS amortization_months_closing       -- 期末償却月数
  FROM acquisition_costs
),

-- レベル5: 減価償却累計額（5時点）の計算
accumulated_depreciation AS (
  SELECT
    *,
    CASE ... END AS accumulated_depreciation_opening,   -- 期首
    CASE ... END AS accumulated_depreciation_increase,  -- 増加
    CASE ... END AS accumulated_depreciation_decrease,  -- 減少
    CASE ... END AS interim_depreciation_expense,       -- 期中
    CASE ... END AS accumulated_depreciation_closing    -- 期末
  FROM amortization_months
),

-- レベル6: 簿価（4時点）の仮計算（減損損失累計額計算前）
book_values_temp AS (
  SELECT
    *,
    acquisition_cost_opening - accumulated_depreciation_opening AS book_value_opening_temp,
    acquisition_cost_increase - accumulated_depreciation_increase AS book_value_increase_temp
  FROM accumulated_depreciation
),

-- レベル7: 減損損失累計額（5時点）の計算
impairment_losses AS (
  SELECT
    *,
    CASE ... END AS impairment_loss_opening,   -- 期首
    -- ★増加減損損失累計額は book_value_opening_temp と book_value_increase_temp を使用
    CASE ... END AS impairment_loss_increase,  -- 増加
    CASE ... END AS impairment_loss_decrease,  -- 減少
    CASE ... END AS interim_impairment_loss,   -- 期中
    CASE ... END AS impairment_loss_closing    -- 期末
  FROM book_values_temp
),

-- レベル8: 最終的な簿価（4時点）の計算
final_book_values AS (
  SELECT
    *,
    CASE ... END AS book_value_opening,   -- 期首簿価
    CASE ... END AS book_value_increase,  -- 増加簿価
    CASE ... END AS book_value_decrease,  -- 減少簿価
    CASE ... END AS book_value_closing    -- 期末簿価
  FROM impairment_losses
)

-- 最終SELECT文
SELECT
  -- 識別情報
  stock_id,
  period_name,
  period_start,
  period_end,

  -- 基礎データ
  part_id,
  part_name,
  supplier_name,
  actual_cost,
  depreciation_period,
  monthly_depreciation,

  -- 日付情報
  inspected_at,
  calculated_first_shipped_at AS first_shipped_at,
  lease_reacquisition_date,
  impossibled_at,
  impairment_date,
  lease_start_at,

  -- フラグ・分類
  sample,
  classification_of_impossibility,

  -- 償却期間
  shokyaku_alpha,
  shokyaku_beta,
  shokyaku_gamma,

  -- 取得原価（4時点）
  acquisition_cost_opening,
  acquisition_cost_increase,
  acquisition_cost_decrease,
  acquisition_cost_closing,

  -- 償却月数（5時点）
  amortization_months_opening,
  amortization_months_depreciation,
  amortization_months_increase,
  amortization_months_decrease,
  amortization_months_closing,

  -- 減価償却累計額（5時点）
  accumulated_depreciation_opening,
  accumulated_depreciation_increase,
  accumulated_depreciation_decrease,
  interim_depreciation_expense,
  accumulated_depreciation_closing,

  -- 減損損失累計額（5時点）
  impairment_loss_opening,
  impairment_loss_increase,
  impairment_loss_decrease,
  interim_impairment_loss,
  impairment_loss_closing,

  -- 簿価（4時点）
  book_value_opening,
  book_value_increase,
  book_value_decrease,
  book_value_closing

FROM final_book_values;
```

---

## 📝 実装ステップ

### Phase 1: 準備（1日目）

#### ステップ1-1: 計算期間マスタテーブルの作成
- [ ] `book_value_calculation_periods` テーブルを作成
- [ ] 期間生成ロジックのテスト
- [ ] 13ヶ月分のデータが正しく生成されることを確認

**検証クエリ**:
```sql
SELECT * FROM `clas-analytics.finance.book_value_calculation_periods`
ORDER BY period_end;
```

**期待結果**: 13行のデータ（2025年11月〜2026年11月）

---

### Phase 2: 基礎データ層の実装（1日目）

#### ステップ2-1: base_stock_data CTEの作成
- [ ] 4テーブルのJOIN（stock, part, supplier, stock_acquisition_costs, fixed_asset_register）
- [ ] 実際の取得原価の計算（cost + overhead_cost - discount）
- [ ] 月次償却額の計算
- [ ] サンプルデータ（stock_id=3）で動作確認

**検証クエリ**:
```sql
WITH base_stock_data AS (
  -- 実装コード
)
SELECT * FROM base_stock_data WHERE stock_id = 3 LIMIT 1;
```

**確認項目**:
- actual_cost = 10281（想定値と一致）
- monthly_depreciation = 10281 / (5 * 12) = 171.35

---

#### ステップ2-2: stock_period_matrix CTEの作成
- [ ] 在庫 × 期間のクロス結合
- [ ] stock_id=3 で13行生成されることを確認

**検証クエリ**:
```sql
WITH ... stock_period_matrix AS (...)
SELECT COUNT(*) FROM stock_period_matrix WHERE stock_id = 3;
```

**期待結果**: 13行

---

### Phase 3: 計算ロジック層の実装（2〜3日目）

#### ステップ3-1: enriched_data CTE（契約識別コード、リース再取得日、初回出荷日）
- [ ] 契約識別コードの抽出ロジック実装
- [ ] リース再取得日の計算ロジック実装（2パターン）
- [ ] 初回出荷日の計算ロジック実装（2パターン）
- [ ] サンプルデータで検証

---

#### ステップ3-2: depreciation_periods CTE（償却α/β/γ）
- [ ] 償却αの実装（Looker Studioロジックを完全移植）
- [ ] 償却βの実装
- [ ] 償却γの実装
- [ ] 減損・貸手リース・除却の優先順位が正しいか確認

**重要ロジック**:
```sql
-- 償却α（期首時点償却済み月数）
CASE
  WHEN 初回出荷日 >= period_start OR 初回出荷日 IS NULL THEN 0
  WHEN impairment_date < period_start
       AND ((impossibled_at IS NULL AND lease_start_at IS NULL)
            OR impossibled_at > impairment_date
            OR lease_start_at > impairment_date)
  THEN GREATEST(
    12 * (EXTRACT(YEAR FROM impairment_date) - EXTRACT(YEAR FROM first_shipped_at))
    + EXTRACT(MONTH FROM impairment_date) - EXTRACT(MONTH FROM first_shipped_at) + 1,
    0
  )
  WHEN lease_start_at < period_start
  THEN GREATEST(
    12 * (EXTRACT(YEAR FROM lease_start_at) - EXTRACT(YEAR FROM first_shipped_at))
    + EXTRACT(MONTH FROM lease_start_at) - EXTRACT(MONTH FROM first_shipped_at),
    0
  )
  WHEN impossibled_at < period_start
       AND classification_of_impossibility = "庫内紛失／棚卸差異"
  THEN GREATEST(
    12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
    + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at) + 1,
    0
  )
  WHEN impossibled_at < period_start
  THEN GREATEST(
    12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
    + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at),
    0
  )
  ELSE
    12 * (EXTRACT(YEAR FROM period_start) - EXTRACT(YEAR FROM first_shipped_at))
    + EXTRACT(MONTH FROM period_start) - EXTRACT(MONTH FROM first_shipped_at)
END AS shokyaku_alpha
```

---

#### ステップ3-3: acquisition_costs CTE（取得原価4時点）
- [ ] 期首取得原価の実装（リース再取得日判定を含む）
- [ ] 増加取得原価の実装（リース再取得 OR 新規入庫）
- [ ] 減少取得原価の実装
- [ ] 期末取得原価の実装（リース再取得日判定を含む）

**重要**: Looker Studioの `DATETIME_TRUNC(リース再取得日, MONTH)` を `DATE_TRUNC(lease_reacquisition_date, MONTH)` に変換

---

#### ステップ3-4: amortization_months CTE（償却月数5時点）
- [ ] 期首償却月数の実装
- [ ] **償却償却月数の実装（β-γ vs β-α の条件分岐）** ★重要
- [ ] 増加償却月数の実装
- [ ] 減少償却月数の実装
- [ ] 期末償却月数の実装

**重要ロジック（償却償却月数）**:
```sql
CASE
  WHEN レベシェア判定 THEN 0
  WHEN inspected_at > period_end THEN 0
  WHEN DATE_TRUNC(lease_reacquisition_date, MONTH) > period_end THEN 0
  WHEN DATE_TRUNC(lease_reacquisition_date, MONTH) >= period_start
  THEN LEAST(depreciation_period * 12, shokyaku_beta)
       - LEAST(depreciation_period * 12, shokyaku_gamma)  -- β - γ
  ELSE LEAST(depreciation_period * 12, shokyaku_beta)
       - LEAST(depreciation_period * 12, shokyaku_alpha)  -- β - α
END
```

---

#### ステップ3-5: accumulated_depreciation CTE（減価償却累計額5時点）
- [ ] 期首減価償却累計額の実装（リース再取得日判定を含む）
- [ ] 増加減価償却累計額の実装
- [ ] 減少減価償却累計額の実装
- [ ] 期中減価償却費の実装（複雑なロジック）
- [ ] 期末減価償却累計額の実装（リース再取得日判定を含む）

---

#### ステップ3-6: book_values_temp CTE（仮簿価計算）
- [ ] 減損損失累計額計算のための仮簿価を計算
- [ ] `book_value_opening_temp = acquisition_cost_opening - accumulated_depreciation_opening`
- [ ] `book_value_increase_temp = acquisition_cost_increase - accumulated_depreciation_increase`

**目的**: 増加減損損失累計額の計算で循環参照を回避

---

#### ステップ3-7: impairment_losses CTE（減損損失累計額5時点）
- [ ] 期首減損損失累計額の実装
- [ ] **増加減損損失累計額の実装（仮簿価を使用）** ★循環参照注意
- [ ] 減少減損損失累計額の実装
- [ ] 期中減損損失の実装
- [ ] 期末減損損失累計額の実装（リース再取得日判定を含む）

**修正後のロジック（増加減損損失累計額）**:
```sql
CASE
  WHEN レベシェア判定 THEN 0
  WHEN impairment_date > DATE_TRUNC(lease_reacquisition_date, MONTH) THEN 0
  WHEN (impairment_date > inspected_at AND lease_reacquisition_date IS NULL) THEN 0
  WHEN impairment_date IS NULL THEN 0
  ELSE acquisition_cost_increase - accumulated_depreciation_increase
END
```

---

#### ステップ3-8: final_book_values CTE（最終簿価4時点）
- [ ] 期首簿価の実装
- [ ] 増加簿価の実装
- [ ] 減少簿価の実装
- [ ] 期末簿価の実装

**計算式**:
```sql
book_value_opening = acquisition_cost_opening
                     - accumulated_depreciation_opening
                     - impairment_loss_opening
```

---

### Phase 4: テーブル作成と検証（4日目）

#### ステップ4-1: 完全なSQL実装
- [ ] 全CTEを統合したCREATE TABLE文の作成
- [ ] 構文エラーのチェック

---

#### ステップ4-2: 小規模テスト実行
- [ ] 特定のstock_id（例: stock_id=3）のみでテスト実行
- [ ] 13ヶ月分のデータが正しく生成されるか確認

**テストクエリ**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.book_value_calculation_test` AS
WITH ... (全ロジック)
SELECT * FROM final_book_values
WHERE stock_id = 3;
```

**確認項目**:
- [ ] 13行生成されるか
- [ ] 期首簿価、期末簿価がLooker Studioと一致するか
- [ ] NULLや異常値がないか

---

#### ステップ4-3: 全データでのテーブル作成
- [ ] 20万件 × 13ヶ月 = 260万件のテーブル作成
- [ ] 実行時間の測定（推定: 5〜10分）
- [ ] エラーの有無を確認

---

#### ステップ4-4: データ品質チェック
- [ ] 総レコード数の確認（260万件程度か）
- [ ] NULLカウント
- [ ] 簿価の合計値が妥当か
- [ ] 特定のstock_idでLooker Studioと比較

**品質チェッククエリ**:
```sql
-- レコード数確認
SELECT COUNT(*) FROM `clas-analytics.finance.book_value_calculation`;

-- 期間別レコード数
SELECT period_name, COUNT(*)
FROM `clas-analytics.finance.book_value_calculation`
GROUP BY period_name
ORDER BY period_name;

-- 簿価の合計（期間別）
SELECT
  period_name,
  SUM(book_value_opening) AS total_opening,
  SUM(book_value_closing) AS total_closing
FROM `clas-analytics.finance.book_value_calculation`
GROUP BY period_name
ORDER BY period_name;

-- NULL値のチェック
SELECT
  COUNTIF(book_value_opening IS NULL) AS null_opening,
  COUNTIF(book_value_closing IS NULL) AS null_closing,
  COUNTIF(actual_cost IS NULL) AS null_cost
FROM `clas-analytics.finance.book_value_calculation`;
```

---

### Phase 5: スケジュール実行設定（5日目）

#### ステップ5-1: スケジュールクエリの作成
- [ ] BigQueryでスケジュールクエリを作成
- [ ] 実行頻度: **毎月1日 AM 3:00**
- [ ] タイムゾーン: Asia/Tokyo
- [ ] 通知設定: エラー時にメール通知

**設定内容**:
```
クエリ名: book_value_calculation_monthly
スケジュール: 毎月1日 3:00
タイムゾーン: Asia/Tokyo
対象クエリ:
  1. CREATE OR REPLACE TABLE book_value_calculation_periods
  2. CREATE OR REPLACE TABLE book_value_calculation
```

---

#### ステップ5-2: 初回手動実行
- [ ] スケジュールクエリの初回実行
- [ ] 実行時間の測定
- [ ] エラーの有無を確認

---

### Phase 6: Looker Studio連携（5日目）

#### ステップ6-1: データソースの作成
- [ ] Looker Studioで新しいデータソースを作成
- [ ] テーブル: `clas-analytics.finance.book_value_calculation`
- [ ] フィルタ項目: `period_name`

---

#### ステップ6-2: ダッシュボードの作成
- [ ] 期間選択フィルタの配置
- [ ] 簿価推移グラフの作成
- [ ] 取得原価、減価償却累計額、減損損失累計額の内訳表示

**推奨グラフ**:
1. 期末簿価の月次推移（折れ線グラフ）
2. 簿価の内訳（積み上げ棒グラフ: 期首簿価 + 増加 - 減少 = 期末簿価）
3. 減価償却費の月次推移
4. カテゴリ別の簿価分布

---

## 🧪 テスト計画

### 単体テスト

#### テスト1: 期間マスタの生成
- [ ] 実行日を2025年12月26日と仮定した場合、2025年11月〜2026年11月の13ヶ月分が生成されるか

#### テスト2: 償却α/β/γの計算
- [ ] stock_id=3で償却α、β、γが正しく計算されるか
- [ ] 減損・貸手リース・除却の優先順位が正しいか

#### テスト3: リース再取得日の判定
- [ ] 株式会社カンムパターンで正しく計算されるか
- [ ] 三井住友トラスト・パナソニックファイナンスパターンで正しく計算されるか

#### テスト4: 償却償却月数のβ-γ vs β-α切り替え
- [ ] リース再取得日が期首〜期末の間ならβ-γが使われるか
- [ ] それ以外はβ-αが使われるか

#### テスト5: 増加減損損失累計額の循環参照回避
- [ ] 仮簿価を使った計算が正しく動作するか

---

### 統合テスト

#### テスト6: Looker Studioとの比較
- [ ] stock_id=3の期末簿価がLooker Studioと一致するか（許容誤差: ±1円）
- [ ] 10件のランダムなstock_idで比較検証

#### テスト7: 全データの整合性
- [ ] 期首簿価 + 増加簿価 - 減少簿価 = 期末簿価が全レコードで成立するか
- [ ] 簿価がマイナスになるレコードがないか

---

## 📊 パフォーマンス最適化

### 想定実行時間
- 計算期間マスタ作成: 1秒未満
- 簿価計算テーブル作成: 5〜10分（20万件 × 13ヶ月）

### 最適化施策（必要に応じて実施）
1. **パーティショニング**: `period_end` でパーティション化
2. **クラスタリング**: `stock_id`, `period_name` でクラスタリング
3. **インデックス**: 頻繁にフィルタする項目

**パーティショニング例**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.book_value_calculation`
PARTITION BY period_end
CLUSTER BY stock_id, period_name
AS ...
```

---

## 🔒 データ品質保証

### データ検証ルール

#### ルール1: 簿価の整合性
```sql
-- 期首簿価 + 増加簿価 - 減少簿価 = 期末簿価
SELECT COUNT(*) AS error_count
FROM `clas-analytics.finance.book_value_calculation`
WHERE ABS(
  book_value_opening + book_value_increase - book_value_decrease - book_value_closing
) > 0.01;  -- 許容誤差: ±0.01円
```

**期待結果**: 0件

---

#### ルール2: 簿価の非負性
```sql
-- 簿価がマイナスでないか
SELECT COUNT(*) AS error_count
FROM `clas-analytics.finance.book_value_calculation`
WHERE book_value_closing < 0;
```

**期待結果**: 0件（減損があればマイナスになる可能性があるため、要確認）

---

#### ルール3: 減価償却累計額 ≤ 取得原価
```sql
SELECT COUNT(*) AS error_count
FROM `clas-analytics.finance.book_value_calculation`
WHERE accumulated_depreciation_closing > acquisition_cost_closing + 0.01;
```

**期待結果**: 0件

---

## 📝 ドキュメント

### 作成すべきドキュメント

1. **技術仕様書** (本ドキュメント)
2. **SQLコード本体** (実装完了後)
3. **運用手順書**:
   - スケジュールクエリの実行確認方法
   - エラー時の対処方法
   - データ再作成手順
4. **Looker Studioダッシュボード利用ガイド**

---

## 🚀 デプロイ計画

### ステップ1: 開発環境でのテスト
- [ ] テスト用テーブル（`book_value_calculation_test`）で検証
- [ ] Looker Studioとの比較検証

### ステップ2: 本番環境へのデプロイ
- [ ] 本番テーブル（`book_value_calculation`）の作成
- [ ] スケジュールクエリの設定

### ステップ3: 監視とメンテナンス
- [ ] 月次でデータ品質チェック
- [ ] 異常値の検出と対処

---

## 📅 実装スケジュール

| フェーズ | 作業内容 | 所要時間 | 担当 |
|---------|---------|---------|------|
| Phase 1 | 準備（期間マスタ作成） | 2時間 | 開発者 |
| Phase 2 | 基礎データ層の実装 | 3時間 | 開発者 |
| Phase 3 | 計算ロジック層の実装 | 8時間 | 開発者 |
| Phase 4 | テーブル作成と検証 | 4時間 | 開発者 |
| Phase 5 | スケジュール実行設定 | 1時間 | 開発者 |
| Phase 6 | Looker Studio連携 | 2時間 | 開発者 |
| **合計** | | **20時間** | |

**推定実装期間**: 3〜5営業日

---

## ⚠️ リスクと対策

### リスク1: 計算ロジックの差異
**内容**: Looker StudioとSQLで計算結果が一致しない

**対策**:
- stock_id=3で逐一比較しながら実装
- 各CTE単位でLooker Studioのロジックと照合

---

### リスク2: 実行時間の超過
**内容**: 260万件の計算に10分以上かかる

**対策**:
- パーティショニング、クラスタリングの導入
- 段階的な実行（CTE単位でのテーブル化）

---

### リスク3: データ品質の問題
**内容**: 異常値、NULL、計算ミスの発生

**対策**:
- データ検証ルールの徹底
- 月次での品質チェック
- アラート設定

---

## 📞 サポート体制

### 問題発生時の連絡先
- 開発担当者: （連絡先）
- BigQuery管理者: （連絡先）
- Looker Studio管理者: （連絡先）

### トラブルシューティング

#### エラー1: スケジュールクエリが失敗
**対処法**:
1. BigQueryのログを確認
2. 手動実行でエラー内容を特定
3. データの変更がないか確認（スキーマ変更など）

#### エラー2: Looker Studioでデータが表示されない
**対処法**:
1. テーブルが正しく作成されているか確認
2. データソースの接続を確認
3. フィルタ設定を確認

---

## ✅ 実装完了チェックリスト

### 必須項目
- [ ] 計算期間マスタテーブルが作成されている
- [ ] 簿価計算テーブルが作成されている（260万件程度）
- [ ] stock_id=3の簿価がLooker Studioと一致している
- [ ] データ検証ルールがすべてパスしている
- [ ] スケジュールクエリが設定されている（毎月1日 AM 3:00）
- [ ] Looker Studioダッシュボードが動作している

### オプション項目
- [ ] パーティショニング、クラスタリングが設定されている
- [ ] 運用手順書が作成されている
- [ ] Looker Studioダッシュボード利用ガイドが作成されている

---

## 🎯 成功基準

### 定量的基準
1. **データ量**: 260万件程度（20万件 × 13ヶ月）
2. **実行時間**: 10分以内
3. **精度**: Looker Studioとの差異が±1円以内
4. **データ品質**: 検証ルールのエラー件数が0件

### 定性的基準
1. Looker Studioで月次推移が可視化できる
2. 期間選択が容易にできる
3. 月次バッチが安定稼働する

---

## 📚 参考資料

- [looker_studio_formulas.md](looker_studio_formulas.md) - Looker Studioの計算式
- [looker_studio_vs_python_comparison.md](looker_studio_vs_python_comparison.md) - Looker StudioとPythonの比較
- [python_logic_analysis.md](python_logic_analysis.md) - Pythonロジックの分析
- [final_data_checklist.md](final_data_checklist.md) - 必要データ項目一覧

---

**作成者**: Claude Code
**最終更新日**: 2025-12-26
**バージョン**: 1.0
