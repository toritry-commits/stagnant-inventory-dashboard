# リース再取得日が各計算項目に与える影響分析

**作成日**: 2025-12-25
**目的**: リース再取得日が簿価計算の各カラムに与える影響を整理し、SQL実装方針を検討

---

## リース再取得の概要

### ビジネス背景

**借手リース**から**自社資産**への再取得（リースバック終了）

1. **初回**: 借手リースとして資産を使用開始（初回出荷日）
2. **リース期間**: 借手リースとして使用（減価償却なし）
3. **再取得**: リース終了時に自社資産として再取得（リース再取得日）
4. **再取得後**: 自社資産として減価償却を再開

### 会計上の扱い

- **リース期間中**: 資産分類は「リース資産(借手リース)」
- **再取得時点**: 取得原価として再度計上（増加取得原価）
- **再取得後**: 減価償却を開始（償却月数γから計算）

---

## リース再取得日が影響する計算項目一覧

| # | 計算項目 | リース再取得日の影響 | 影響内容 | 使用する関数（Python） |
|---|---------|-------------------|---------|---------------------|
| 1 | **償却γ** | ✅ 直接使用 | 初回出荷日からリース再取得日までの経過月数 | `calculate_shokyaku_gamma()` |
| 2 | **期首取得原価** | ✅ 条件判定 | リース再取得日が期首日より前なら0 | `calculate_initial_cost()` |
| 3 | **増加取得原価** | ✅ 条件判定 | リース再取得日が期首〜期末の間なら取得原価を計上 | `calculate_acquisition_cost_increase()` |
| 4 | **減少取得原価** | ✅ 条件判定 | 除売却・貸手リース転換がリース再取得日より前なら0 | `calculate_acquisition_cost_decrease()` |
| 5 | **償却償却月数** | ✅ 条件判定 | リース再取得日が期首〜期末の間なら償却β-償却γ | `calculate_amortization_months_shokyaku()` |
| 6 | **増加償却月数** | ✅ 条件判定 | リース再取得日が期首〜期末の間なら償却γ | `calculate_amortization_months_increase()` |
| 7 | **減少償却月数** | ✅ 条件判定 | 除売却・貸手リース転換がリース再取得日より前なら0 | `calculate_amortization_months_decrease()` |
| 8 | **期首減価償却累計額** | ✅ 条件判定 | リース再取得日が期首日以降なら0 | `calculate_accumulated_depreciation_kishu()` |
| 9 | **期末減価償却累計額** | ✅ 条件判定 | リース再取得日が期末日より後なら0 | `calculate_accumulated_depreciation_kimatsu()` |
| 10 | **期首減損損失累計額** | ❌ 間接影響 | 減損日とリース再取得日の前後関係で判定 | `calculate_new_impairment_loss_kishu()` |
| 11 | **期末減損損失累計額** | ✅ 条件判定 | リース再取得日が期末日より後なら0 | `calculate_new_impairment_loss_kimatsu()` |
| 12 | **増加減損損失累計額** | ✅ 条件判定 | 減損日がリース再取得日より後なら0 | `calculate_new_impairment_loss_increase()` |

---

## 詳細分析: 各計算項目への影響

### 1. 償却γ（リース再取得時点の償却済み月数）

**目的**: リース再取得時点で何ヶ月分の減価償却が完了しているかを計算

**計算ロジック**:
```python
def calculate_shokyaku_gamma(row):
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    if pd.isna(lease_reacquisition_date): return 0

    # リース再取得日の月初
    lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)

    # 特殊ケース: 減損・除売却・貸手リース転換がリース再取得日より前
    if 減損損失日 < リース再取得日:
        return 減損時点の償却月数
    if 貸手リース開始日 < リース再取得日:
        return 貸手リース開始時点の償却月数
    if 除売却日 < リース再取得日:
        return 除売却時点の償却月数

    # 通常ケース: 初回出荷日からリース再取得日までの経過月数
    return DATE_DIFF(リース再取得日, 初回出荷日, MONTH)
```

**SQLでの実装**:
```sql
CASE
  WHEN lease_reacquisition_date IS NULL THEN 0

  -- 減損がリース再取得より前
  WHEN impairment_date IS NOT NULL
       AND impairment_date < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN DATE_DIFF(impairment_date, first_shipped_at, MONTH) + 1

  -- 貸手リース転換がリース再取得より前
  WHEN lease_start_at IS NOT NULL
       AND lease_start_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN DATE_DIFF(lease_start_at, first_shipped_at, MONTH)

  -- 除売却がリース再取得より前
  WHEN impossibled_at IS NOT NULL
       AND impossibled_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN CASE
    WHEN classification_of_impossibility = '庫内紛失／棚卸差異'
    THEN DATE_DIFF(impossibled_at, first_shipped_at, MONTH) + 1
    ELSE DATE_DIFF(impossibled_at, first_shipped_at, MONTH)
  END

  -- 通常ケース
  ELSE DATE_DIFF(lease_reacquisition_date, first_shipped_at, MONTH)
END AS shokyaku_gamma
```

---

### 2. 期首取得原価

**影響**: リース再取得日が期首日以降の場合、期首時点ではまだ自社資産ではないため0

**計算ロジック**:
```python
def calculate_initial_cost(row, start_date_param):
    # ... (省略) ...

    # リース再取得日が期首日以降なら0（まだ借手リース中）
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
        if lease_reacquisition_date_month_start >= start_date_param:
            return 0

    return cost
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日が期首日以降 → まだ借手リース中なので0
  WHEN lease_reacquisition_date IS NOT NULL
       AND DATE_TRUNC(lease_reacquisition_date, MONTH) >= @start_date
  THEN 0

  -- その他の条件（サプライヤー名判定、入庫日チェックなど）
  -- ...

  ELSE actual_cost
END AS acquisition_cost_kishu
```

**ポイント**: リース再取得日がある = 借手リースから自社資産への転換 = 期首時点で未保有

---

### 3. 増加取得原価

**影響**: リース再取得日が期首〜期末の間にある場合、その時点で取得原価を計上

**計算ロジック**:
```python
def calculate_acquisition_cost_increase(row, start_date_param, end_date_param):
    # リース再取得があり、期首〜期末の間なら取得原価を計上
    if pd.notna(lease_reacquisition_date) and (start_date_param <= lease_reacquisition_date <= end_date_param):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)

        # 除売却・貸手リース転換がリース再取得日以降であることを確認
        condition_impossibled_ok = (
            pd.isna(impossibled_at) or
            impossibled_at >= lease_reacquisition_date_month_start
        )
        condition_lease_shipped_ok = (
            pd.isna(lease_first_shipped_at) or
            lease_first_shipped_at >= lease_reacquisition_date_month_start
        )

        if condition_impossibled_ok and condition_lease_shipped_ok:
            return cost

    # 通常の入庫による増加
    if pd.notna(inspected_at) and (start_date_param <= inspected_at <= end_date_param) and pd.isna(lease_reacquisition_date):
        return cost

    return 0
```

**SQLでの実装**:
```sql
CASE
  -- ケース1: リース再取得による増加
  WHEN lease_reacquisition_date IS NOT NULL
       AND @start_date <= lease_reacquisition_date
       AND lease_reacquisition_date <= @end_date
       AND (impossibled_at IS NULL OR impossibled_at >= DATE_TRUNC(lease_reacquisition_date, MONTH))
       AND (lease_start_at IS NULL OR lease_start_at >= DATE_TRUNC(lease_reacquisition_date, MONTH))
  THEN actual_cost

  -- ケース2: 通常の入庫による増加（リース再取得がない場合のみ）
  WHEN lease_reacquisition_date IS NULL
       AND inspected_at IS NOT NULL
       AND @start_date <= inspected_at
       AND inspected_at <= @end_date
  THEN actual_cost

  ELSE 0
END AS acquisition_cost_increase
```

**ポイント**: リース再取得 = 新規取得として扱う

---

### 4. 減少取得原価

**影響**: 除売却・貸手リース転換がリース再取得日より前なら0（まだ借手リース中なので自社資産ではない）

**計算ロジック**:
```python
def calculate_acquisition_cost_decrease(row, start_date_param, end_date_param):
    # リース再取得日がある場合、除売却等がそれより前なら0
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)

        if pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start:
            return 0
        if pd.notna(lease_first_shipped_at) and lease_first_shipped_at < lease_reacquisition_date_month_start:
            return 0

    # 通常の減少判定
    decrease_due_to_impossibled = (
        pd.notna(impossibled_at) and
        (start_date_param <= impossibled_at <= end_date_param)
    )
    decrease_due_to_lease_transfer = (
        pd.notna(lease_first_shipped_at) and
        (start_date_param <= lease_first_shipped_at <= end_date_param)
    )

    if decrease_due_to_impossibled or decrease_due_to_lease_transfer:
        return cost

    return 0
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日より前の除売却 → 0（まだ借手リース中）
  WHEN lease_reacquisition_date IS NOT NULL
       AND impossibled_at IS NOT NULL
       AND impossibled_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN 0

  -- リース再取得日より前の貸手リース転換 → 0
  WHEN lease_reacquisition_date IS NOT NULL
       AND lease_start_at IS NOT NULL
       AND lease_start_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN 0

  -- 期首〜期末の間に除売却
  WHEN impossibled_at IS NOT NULL
       AND @start_date <= impossibled_at
       AND impossibled_at <= @end_date
  THEN actual_cost

  -- 期首〜期末の間に貸手リース転換
  WHEN lease_start_at IS NOT NULL
       AND @start_date <= lease_start_at
       AND lease_start_at <= @end_date
  THEN actual_cost

  ELSE 0
END AS acquisition_cost_decrease
```

---

### 5. 償却償却月数（期中に償却した月数）

**影響**: リース再取得日が期首〜期末の間にある場合、償却β - 償却γ

**計算ロジック**:
```python
def calculate_amortization_months_shokyaku(row, start_date_param, end_date_param):
    shokyaku_alpha = row.get(SHOKYAKU_ALPHA_COL, 0)
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    shokyaku_gamma = row.get(SHOKYAKU_GAMMA_COL, 0)

    # リース再取得日が期首〜期末の間にある場合
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start >= start_date_param:
        # 償却β - 償却γ（リース再取得後の償却月数）
        val = min(depreciation_period_months, shokyaku_beta) - min(depreciation_period_months, shokyaku_gamma)
        return max(0, val)
    else:
        # 通常ケース: 償却β - 償却α
        val = min(depreciation_period_months, shokyaku_beta) - min(depreciation_period_months, shokyaku_alpha)
        return max(0, val)
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日が期首日以降
  WHEN lease_reacquisition_date IS NOT NULL
       AND DATE_TRUNC(lease_reacquisition_date, MONTH) >= @start_date
  THEN GREATEST(0,
    LEAST(shokyaku_beta, depreciation_period * 12) -
    LEAST(shokyaku_gamma, depreciation_period * 12)
  )

  -- 通常ケース
  ELSE GREATEST(0,
    LEAST(shokyaku_beta, depreciation_period * 12) -
    LEAST(shokyaku_alpha, depreciation_period * 12)
  )
END AS amortization_months_shokyaku
```

**ポイント**: リース再取得後の期間のみを償却月数として計算

---

### 6. 増加償却月数

**影響**: リース再取得日が期首〜期末の間にある場合、償却γを計上

**計算ロジック**:
```python
def calculate_amortization_months_increase(row, start_date_param, end_date_param):
    shokyaku_gamma = row.get(SHOKYAKU_GAMMA_COL, 0)

    # リース再取得日が期首〜期末の間にあり、
    # 除売却・貸手リース転換がリース再取得日より前でない場合
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)

    condition_reacquisition_out_of_period = (
        pd.isna(lease_reacquisition_date_month_start) or
        lease_reacquisition_date_month_start < start_date_param or
        lease_reacquisition_date_month_start > end_date_param
    )

    condition_event_before_reacquisition = False
    if pd.notna(lease_reacquisition_date_month_start):
        if (pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start) or \
           (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < lease_reacquisition_date_month_start):
            condition_event_before_reacquisition = True

    if condition_reacquisition_out_of_period or condition_event_before_reacquisition:
        return 0

    return min(shokyaku_gamma, depreciation_period_months)
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日が期首〜期末の間
  WHEN lease_reacquisition_date IS NOT NULL
       AND @start_date <= DATE_TRUNC(lease_reacquisition_date, MONTH)
       AND DATE_TRUNC(lease_reacquisition_date, MONTH) <= @end_date
       -- 除売却・貸手リース転換がリース再取得日より前でない
       AND (impossibled_at IS NULL OR impossibled_at >= DATE_TRUNC(lease_reacquisition_date, MONTH))
       AND (lease_start_at IS NULL OR lease_start_at >= DATE_TRUNC(lease_reacquisition_date, MONTH))
  THEN LEAST(shokyaku_gamma, depreciation_period * 12)

  ELSE 0
END AS amortization_months_increase
```

---

### 7. 減少償却月数

**影響**: 除売却・貸手リース転換がリース再取得日より前なら0

**計算ロジック**:
```python
def calculate_amortization_months_decrease(row, start_date_param, end_date_param):
    # リース再取得日がある場合、除売却等がそれより前なら0
    if pd.notna(lease_reacquisition_date_month_start):
        if (pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start) or \
           (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < lease_reacquisition_date_month_start):
            return 0

    # 通常の減少判定
    # ...

    return min(shokyaku_beta, depreciation_period_months)
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日より前の除売却 → 0
  WHEN lease_reacquisition_date IS NOT NULL
       AND impossibled_at IS NOT NULL
       AND impossibled_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN 0

  -- リース再取得日より前の貸手リース転換 → 0
  WHEN lease_reacquisition_date IS NOT NULL
       AND lease_start_at IS NOT NULL
       AND lease_start_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN 0

  -- 通常の減少判定
  -- ...

  ELSE LEAST(shokyaku_beta, depreciation_period * 12)
END AS amortization_months_decrease
```

---

### 8. 期首減価償却累計額

**影響**: リース再取得日が期首日以降なら0（まだ借手リース中）

**計算ロジック**:
```python
def calculate_accumulated_depreciation_kishu(row, start_date_param):
    # リース再取得日が期首日以降なら0
    if pd.notna(lease_reacquisition_date_month_start) and start_date_param <= lease_reacquisition_date_month_start:
        return 0

    # 通常の計算
    # ...
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日が期首日以降 → 0
  WHEN lease_reacquisition_date IS NOT NULL
       AND @start_date <= DATE_TRUNC(lease_reacquisition_date, MONTH)
  THEN 0

  -- 通常の計算
  -- ...
END AS accumulated_depreciation_kishu
```

---

### 9. 期末減価償却累計額

**影響**: リース再取得日が期末日より後なら0

**計算ロジック**:
```python
def calculate_accumulated_depreciation_kimatsu(row, end_date_param):
    # リース再取得日が期末日より後なら0
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start > end_date_param:
        return 0

    # 通常の計算
    # ...
```

**SQLでの実装**:
```sql
CASE
  -- リース再取得日が期末日より後 → 0
  WHEN lease_reacquisition_date IS NOT NULL
       AND DATE_TRUNC(lease_reacquisition_date, MONTH) > @end_date
  THEN 0

  -- 通常の計算
  -- ...
END AS accumulated_depreciation_kimatsu
```

---

### 10-12. 減損損失累計額

**影響**: 減損損失日とリース再取得日の前後関係で判定

**共通ロジック**: 減損損失日がリース再取得日より後の場合、増加減損損失累計額は0（まだ借手リース中だった）

---

## リース再取得の典型的なケース

### ケース1: 期中にリース再取得

```
タイムライン:
2022-05-01: 初回出荷（借手リース開始）
2024-05-01: リース再取得 ← 期首〜期末の間
2025-03-01: 期首日
2026-02-28: 期末日

計算結果:
- 期首取得原価: 取得原価（既に再取得済み）
- 増加取得原価: 0（期首より前に再取得済み）
- 償却α: DATE_DIFF(2025-03-01, 2022-05-01, MONTH) = 34ヶ月
- 償却β: DATE_DIFF(2026-02-28, 2022-05-01, MONTH) + 1 = 46ヶ月
- 償却γ: DATE_DIFF(2024-05-01, 2022-05-01, MONTH) = 24ヶ月
- 償却償却月数: 償却β - 償却α = 12ヶ月
```

### ケース2: 期首〜期末の間にリース再取得

```
タイムライン:
2022-05-01: 初回出荷（借手リース開始）
2025-03-01: 期首日
2025-08-01: リース再取得 ← 期首〜期末の間
2026-02-28: 期末日

計算結果:
- 期首取得原価: 0（まだ借手リース中）
- 増加取得原価: 取得原価（期中に再取得）
- 償却α: DATE_DIFF(2025-03-01, 2022-05-01, MONTH) = 34ヶ月
- 償却β: DATE_DIFF(2026-02-28, 2022-05-01, MONTH) + 1 = 46ヶ月
- 償却γ: DATE_DIFF(2025-08-01, 2022-05-01, MONTH) = 40ヶ月
- 償却償却月数: 償却β - 償却γ = 6ヶ月（再取得後のみ）
- 増加償却月数: 償却γ = 40ヶ月
```

---

## まとめ表: リース再取得日の影響

| カラム | リース再取得日がない | リース再取得日 < 期首日 | 期首日 ≤ リース再取得日 ≤ 期末日 | リース再取得日 > 期末日 |
|-------|-------------------|---------------------|----------------------------|---------------------|
| **期首取得原価** | 通常計算 | 取得原価 | 0 | 0 |
| **増加取得原価** | 入庫日で判定 | 0 | 取得原価 | 0 |
| **減少取得原価** | 除売却日で判定 | 除売却日で判定 | 除売却日で判定（再取得日以降のみ） | 0 |
| **償却α** | 通常計算 | 通常計算 | 通常計算 | 通常計算 |
| **償却β** | 通常計算 | 通常計算 | 通常計算 | 通常計算 |
| **償却γ** | 0 | 再取得時の月数 | 再取得時の月数 | 再取得時の月数 |
| **償却償却月数** | 償却β - 償却α | 償却β - 償却α | **償却β - 償却γ** | 償却β - 償却α |
| **増加償却月数** | 0 | 0 | **償却γ** | 0 |
| **減少償却月数** | 除売却日で判定 | 除売却日で判定 | 除売却日で判定（再取得日以降のみ） | 0 |
| **期首減価償却累計額** | 通常計算 | 通常計算 | **0** | **0** |
| **期末減価償却累計額** | 通常計算 | 通常計算 | 通常計算 | **0** |

---

## SQL実装の方針案

### 方針A: リース再取得日を常に計算する

すべてのstock_idに対してリース再取得日を計算し、条件分岐で使用

**メリット**:
- Pythonロジックと完全一致
- 計算の透明性が高い

**デメリット**:
- サプライヤー名パターンマッチのコストが全レコードに発生
- 実際にリース再取得があるのは一部のレコードのみ

### 方針B: リース対象のみ計算する

サプライヤー名で絞り込んでからリース再取得日を計算

**メリット**:
- パフォーマンスが良い
- 不要な計算を削減

**デメリット**:
- 条件分岐が複雑になる

### 方針C: 事前計算してテーブルに保存

リース再取得日を事前に計算してlake.stockまたは別テーブルに保存

**メリット**:
- クエリがシンプルになる
- 実行時のパフォーマンスが最良

**デメリット**:
- データ更新の手間が増える
- リアルタイム性が失われる

---

## 推奨実装方針

**方針A（リース再取得日を常に計算）** を推奨

理由:
1. Pythonロジックとの完全一致を保証
2. BigQueryの最適化によりパフォーマンス問題は発生しにくい
3. 将来的にリース再取得のパターンが増えても対応可能
4. 計算ロジックが明示的で保守性が高い

実装順序:
1. 契約識別コードの抽出
2. リース再取得日の計算
3. 各計算項目でリース再取得日を条件判定に使用
