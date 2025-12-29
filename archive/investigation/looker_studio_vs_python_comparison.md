# Looker Studio vs Python 実装比較分析

**作成日**: 2025-12-26
**目的**: Looker Studioの実装とPythonロジックの差異を特定し、SQL実装時の正しいロジックを確定する

---

## 🔍 主要な差異サマリー

### 1️⃣ **リース再取得日の扱い（最重要差異）**

| 項目 | Looker Studio | Python | 推奨 |
|-----|--------------|--------|------|
| 期首取得原価 | ✅ リース再取得日を考慮<br>`期首日 <= DATETIME_TRUNC(リース再取得日, MONTH)` | ❌ リース再取得日の考慮なし | **Looker Studio** |
| 増加取得原価 | ✅ リース再取得を明示的に判定<br>`期首日 <= リース再取得日 <= 期末日` | ✅ 同様のロジック | 両方OK |
| 償却償却月数 | ✅ **β-γ** を使用（リース再取得が期首〜期末の間） | ✅ **β-γ** を使用 | 両方OK |
| 期首減価償却累計額 | ✅ リース再取得日を考慮<br>`期首日 <= DATETIME_TRUNC(リース再取得日, MONTH)` なら0 | ❌ リース再取得日の考慮なし | **Looker Studio** |

**結論**: Looker Studioの方がリース再取得日の影響を正しく反映している

---

### 2️⃣ **償却α/β/γの計算ロジック差異**

#### 償却α（期首償却月数）

**Looker Studio**:
```sql
CASE
    WHEN 初回出荷日 >= 計算基準日(期首日) or 初回出荷日 is NULL THEN 0
    -- 減損が期首前に発生している場合
    WHEN impairment_date < 計算基準日(期首日)
         AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL)
              OR impossibled_at > impairment_date
              OR lease_first_shipped_at > impairment_date)
    THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日))
                  + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    -- 貸手リースが期首前に開始している場合
    WHEN lease_first_shipped_at < 計算基準日(期首日)
    THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日))
                  + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    -- 除却が期首前で「庫内紛失／棚卸差異」の場合
    WHEN impossibled_at < 計算基準日(期首日)
         and classification_of_impossibility = "庫内紛失／棚卸差異"
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)
    -- 除却が期首前（通常）の場合
    WHEN impossibled_at < 計算基準日(期首日)
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日), 0)
    -- デフォルト: 初回出荷日から期首日までの月数
    ELSE 12*(YEAR(計算基準日(期首日)) - YEAR(初回出荷日))
         + MONTH(計算基準日(期首日)) - MONTH(初回出荷日)
END
```

**Python**:
```python
def calculate_shokyaku_alpha(row, start_date_param):
    first_shipped_at = row['供与開始日(初回出荷日)']
    start_date = pd.to_datetime(start_date_param)

    if pd.isna(first_shipped_at) or first_shipped_at >= start_date:
        return 0

    # 減損損失の判定
    impairment_date = row.get('減損損失日')
    if pd.notna(impairment_date) and impairment_date < start_date:
        impossibled_at = row.get('除売却日')
        lease_first_shipped_at = row.get('貸手リース開始日')
        if (pd.isna(impossibled_at) and pd.isna(lease_first_shipped_at)) or \
           (pd.notna(impossibled_at) and impossibled_at > impairment_date) or \
           (pd.notna(lease_first_shipped_at) and lease_first_shipped_at > impairment_date):
            months = (impairment_date.year - first_shipped_at.year) * 12 + \
                     (impairment_date.month - first_shipped_at.month) + 1
            return max(months, 0)

    # 貸手リース開始の判定
    lease_first_shipped_at = row.get('貸手リース開始日')
    if pd.notna(lease_first_shipped_at) and lease_first_shipped_at < start_date:
        months = (lease_first_shipped_at.year - first_shipped_at.year) * 12 + \
                 (lease_first_shipped_at.month - first_shipped_at.month)
        return max(months, 0)

    # 除却の判定
    impossibled_at = row.get('除売却日')
    classification = row.get('破損紛失分類', '')
    if pd.notna(impossibled_at) and impossibled_at < start_date:
        if classification == '庫内紛失／棚卸差異':
            months = (impossibled_at.year - first_shipped_at.year) * 12 + \
                     (impossibled_at.month - first_shipped_at.month) + 1
        else:
            months = (impossibled_at.year - first_shipped_at.year) * 12 + \
                     (impossibled_at.month - first_shipped_at.month)
        return max(months, 0)

    # デフォルト
    months = (start_date.year - first_shipped_at.year) * 12 + \
             (start_date.month - first_shipped_at.month)
    return months
```

**差異**:
- ✅ ロジックは完全に一致
- Looker StudioはCASE文、PythonはIF文で同じ条件分岐を実装
- 月数計算も同じ（`+1`のタイミングも一致）

---

#### 償却β（期末償却月数）

**Looker Studio**:
```sql
CASE
    WHEN 初回出荷日 > 計算基準日(期末日) or 初回出荷日 is NULL THEN 0
    WHEN impairment_date <= 計算基準日(期末日)
         AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL)
              OR impossibled_at > impairment_date
              OR lease_first_shipped_at > impairment_date)
    THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日))
                  + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    WHEN lease_first_shipped_at <= 計算基準日(期末日)
    THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日))
                  + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    WHEN impossibled_at <= 計算基準日(期末日)
         and classification_of_impossibility = "庫内紛失／棚卸差異"
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)
    WHEN impossibled_at <= 計算基準日(期末日)
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日), 0)
    ELSE 12*(YEAR(計算基準日(期末日)) - YEAR(初回出荷日))
         + MONTH(計算基準日(期末日)) - MONTH(初回出荷日) + 1
END
```

**Python**:
```python
def calculate_shokyaku_beta(row, end_date_param):
    first_shipped_at = row['供与開始日(初回出荷日)']
    end_date = pd.to_datetime(end_date_param)

    if pd.isna(first_shipped_at) or first_shipped_at > end_date:
        return 0

    # 減損、貸手リース、除却の判定（同様の構造）
    # ...

    # デフォルト
    months = (end_date.year - first_shipped_at.year) * 12 + \
             (end_date.month - first_shipped_at.month) + 1
    return months
```

**差異**:
- ✅ ロジックは完全に一致
- デフォルトケースで `+1` を追加（期末は翌月初の前日扱い）

---

#### 償却γ（リース再取得時償却月数）

**Looker Studio**:
```sql
CASE
    WHEN リース資産再取得日 is null THEN 0
    WHEN impairment_date < DATETIME_TRUNC(リース資産再取得日, MONTH)
         AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL)
              OR impossibled_at > impairment_date
              OR lease_first_shipped_at > impairment_date)
    THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日))
                  + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    WHEN lease_first_shipped_at < DATETIME_TRUNC(リース資産再取得日, MONTH)
    THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日))
                  + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    WHEN impossibled_at < DATETIME_TRUNC(リース資産再取得日, MONTH)
         and classification_of_impossibility = "庫内紛失／棚卸差異"
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)
    WHEN impossibled_at < DATETIME_TRUNC(リース資産再取得日, MONTH)
    THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日))
                  + MONTH(impossibled_at) - MONTH(初回出荷日), 0)
    ELSE 12*(YEAR(リース資産再取得日) - YEAR(初回出荷日))
         + MONTH(リース資産再取得日) - MONTH(初回出荷日)
END
```

**Python**:
```python
def calculate_shokyaku_gamma(row):
    lease_reacquisition_date = row.get('リース再取得日')
    if pd.isna(lease_reacquisition_date):
        return 0

    first_shipped_at = row['供与開始日(初回出荷日)']
    # 月初に正規化
    lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)

    # 減損、貸手リース、除却の判定（同様の構造）
    # ...

    # デフォルト
    months = (lease_reacquisition_date.year - first_shipped_at.year) * 12 + \
             (lease_reacquisition_date.month - first_shipped_at.month)
    return months
```

**差異**:
- ✅ ロジックは完全に一致
- Looker Studioは `DATETIME_TRUNC(リース資産再取得日, MONTH)` で月初化
- Pythonは `.replace(day=1)` で月初化

---

### 3️⃣ **取得原価の条件分岐差異**

#### 期首取得原価

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    WHEN inspected_at is null THEN 0
    WHEN 計算基準日(期首日) <= inspected_at THEN 0
    WHEN 計算基準日(期首日) <= DATETIME_TRUNC(リース資産再取得日, MONTH) THEN 0  -- ⭐新規
    WHEN impossibled_at < 計算基準日(期首日)
         or lease_first_shipped_at < 計算基準日(期首日) THEN 0
    ELSE 取得原価
END
```

**Python**:
```python
def calculate_initial_cost(row, start_date_param):
    # レベシェア判定
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    inspected_at = row.get('入庫検品完了日')
    if pd.isna(inspected_at):
        return 0

    start_date = pd.to_datetime(start_date_param)
    if inspected_at >= start_date:
        return 0

    # ⭐ リース再取得日の判定なし

    impossibled_at = row.get('除売却日')
    lease_first_shipped_at = row.get('貸手リース開始日')
    if (pd.notna(impossibled_at) and impossibled_at < start_date) or \
       (pd.notna(lease_first_shipped_at) and lease_first_shipped_at < start_date):
        return 0

    return row['取得原価']
```

**差異**:
- 🔴 **Looker Studioのみ**: `期首日 <= DATETIME_TRUNC(リース再取得日, MONTH)` の判定あり
- **意味**: リース再取得日が期首以降なら、期首時点ではまだリース中なので取得原価は0

**推奨**: **Looker Studioのロジックが正しい**

---

#### 増加取得原価

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    -- リース再取得パターン
    WHEN (計算基準日(期首日) <= リース資産再取得日
          and リース資産再取得日 <= 計算基準日(期末日)
          and (impossibled_at is null or impossibled_at >= DATETIME_TRUNC(リース資産再取得日, MONTH))
          and (lease_first_shipped_at is null or lease_first_shipped_at >= DATETIME_TRUNC(リース資産再取得日, MONTH)))
    Then 取得原価
    -- 通常の入庫パターン
    WHEN (計算基準日(期首日) <= inspected_at
          and inspected_at <= 計算基準日(期末日))
    then 取得原価
    ELSE 0
END
```

**Python**:
```python
def calculate_acquisition_cost_increase(row, start_date_param, end_date_param):
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    start_date = pd.to_datetime(start_date_param)
    end_date = pd.to_datetime(end_date_param)

    # リース再取得パターン
    lease_reacquisition_date = row.get('リース再取得日')
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_month_start = lease_reacquisition_date.replace(day=1)
        if start_date <= lease_reacquisition_date <= end_date:
            impossibled_at = row.get('除売却日')
            lease_first_shipped_at = row.get('貸手リース開始日')
            if (pd.isna(impossibled_at) or impossibled_at >= lease_reacquisition_month_start) and \
               (pd.isna(lease_first_shipped_at) or lease_first_shipped_at >= lease_reacquisition_month_start):
                return row['取得原価']

    # 通常の入庫パターン
    inspected_at = row.get('入庫検品完了日')
    if pd.notna(inspected_at) and start_date <= inspected_at <= end_date:
        return row['取得原価']

    return 0
```

**差異**:
- ✅ ロジックは完全に一致

---

### 4️⃣ **償却償却月数（期中償却月数）**

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    WHEN inspected_at > 計算基準日(期末日) THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month) >= 計算基準日(期首日)
    THEN NARY_MIN(depreciation_period*12, 償却β) - NARY_MIN(depreciation_period*12, 償却γ)
    ELSE NARY_MIN(depreciation_period*12, 償却β) - NARY_MIN(depreciation_period*12, 償却α)
END
```

**Python**:
```python
def calculate_amortization_months_shokyaku(row, start_date_param, end_date_param):
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    inspected_at = row.get('入庫検品完了日')
    end_date = pd.to_datetime(end_date_param)
    if pd.notna(inspected_at) and inspected_at > end_date:
        return 0

    # リース再取得日の判定
    lease_reacquisition_date = row.get('リース再取得日')
    start_date = pd.to_datetime(start_date_param)

    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_month_start = lease_reacquisition_date.replace(day=1)
        if lease_reacquisition_month_start > end_date:
            return 0
        if lease_reacquisition_month_start >= start_date:
            # β - γ を使用
            beta = row['償却β']
            gamma = row['償却γ']
            depreciation_period = row['耐用年数']
            return min(depreciation_period * 12, beta) - min(depreciation_period * 12, gamma)

    # β - α を使用
    beta = row['償却β']
    alpha = row['償却α']
    depreciation_period = row['耐用年数']
    return min(depreciation_period * 12, beta) - min(depreciation_period * 12, alpha)
```

**差異**:
- ✅ ロジックは完全に一致
- **重要**: リース再取得が期首〜期末の間なら **β-γ**、それ以外は **β-α** を使用

---

### 5️⃣ **期首減価償却累計額**

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    WHEN 償却月数(期首) = 0 THEN 0
    WHEN 計算基準日(期首日) <= DATETIME_TRUNC(リース資産再取得日, MONTH) THEN 0  -- ⭐新規
    WHEN 取得原価 < NARY_MIN(depreciation_period*12, 償却α)
         or depreciation_period*12 <= 償却α
    THEN 取得原価
    ELSE 月次償却額(会計) * 償却月数(期首)
END
```

**Python**:
```python
def calculate_accumulated_depreciation_kishu(row, start_date_param):
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    amortization_months_kishu = row['償却月数(期首)']
    if amortization_months_kishu == 0:
        return 0

    # ⭐ リース再取得日の判定なし

    acquisition_cost = row['取得原価']
    depreciation_period = row['耐用年数']
    alpha = row['償却α']

    if acquisition_cost < min(depreciation_period * 12, alpha) or \
       depreciation_period * 12 <= alpha:
        return acquisition_cost

    monthly_depreciation = row['月次償却額']
    return monthly_depreciation * amortization_months_kishu
```

**差異**:
- 🔴 **Looker Studioのみ**: `期首日 <= DATETIME_TRUNC(リース再取得日, MONTH)` の判定あり
- **意味**: リース再取得日が期首以降なら、期首時点での減価償却累計額は0（まだリース中）

**推奨**: **Looker Studioのロジックが正しい**

---

### 6️⃣ **減損損失の計算差異**

#### 期首減損損失累計額

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    ELSE IF(impairment_date < 計算基準日(期首日),
            取得原価(期首) - 減価償却累計額(期首), 0)
END
```

**Python**:
```python
def calculate_new_impairment_loss_kishu(row, start_date_param):
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    impairment_date = row.get('減損損失日')
    start_date = pd.to_datetime(start_date_param)

    if pd.notna(impairment_date) and impairment_date < start_date:
        return row['取得原価(期首)'] - row['減価償却累計額(期首)']

    return 0
```

**差異**:
- ✅ ロジックは完全に一致

---

#### 増加減損損失累計額

**Looker Studio**:
```sql
CASE
    WHEN (レベシェア判定) or sample THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0  -- ⭐新規
    WHEN inspected_at <= 計算基準日(期末日)
         and 計算基準日(期首日) <= impairment_date
         and impairment_date <= 計算基準日(期末日)
    THEN 簿価(期首) + 簿価(増加) - 減価償却累計額(償却)
    ELSE 0
END
```

**Python**:
```python
def calculate_new_impairment_loss_increase(row, start_date_param, end_date_param):
    if is_revenue_share_item(row) or row.get('sample'):
        return 0

    # ⭐ リース再取得日の判定なし

    inspected_at = row.get('入庫検品完了日')
    impairment_date = row.get('減損損失日')
    start_date = pd.to_datetime(start_date_param)
    end_date = pd.to_datetime(end_date_param)

    if pd.notna(inspected_at) and inspected_at <= end_date and \
       pd.notna(impairment_date) and start_date <= impairment_date <= end_date:
        return row['簿価(期首)'] + row['簿価(増加)'] - row['減価償却累計額(償却)']

    return 0
```

**差異**:
- 🔴 **Looker Studioのみ**: `DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日)` の判定あり
- **意味**: リース再取得日が期末より後なら、増加減損損失は0

**推奨**: **Looker Studioのロジックが正しい**

---

### 7️⃣ **期末関連項目のリース再取得日判定**

以下の項目でLooker Studioのみリース再取得日の判定が追加されています:

- **期末取得原価**: `DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0`
- **期末減価償却累計額**: `DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0`
- **期末減損損失累計額**: `DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0`
- **期中減損損失**: `DATETIME_TRUNC(リース資産再取得日,month) > 計算基準日(期末日) THEN 0`

**意味**: リース再取得日が期末より後なら、期末時点ではまだリース中のため計算対象外

**推奨**: **Looker Studioのロジックが正しい**

---

## 📊 差異の影響度評価

| 差異カテゴリ | 影響度 | 影響を受ける項目数 | 推奨実装 |
|------------|--------|------------------|---------|
| リース再取得日の考慮（期首） | 🔴 **高** | 2項目 | Looker Studio |
| リース再取得日の考慮（期末） | 🔴 **高** | 5項目 | Looker Studio |
| 償却α/β/γの計算 | 🟢 低 | 3項目 | 両方同じ |
| 取得原価の条件分岐 | 🟢 低 | 4項目 | 両方同じ |
| 減損損失の計算 | 🟡 中 | 1項目 | Looker Studio |

---

## ✅ SQL実装時の推奨ロジック

### **採用すべき実装**: Looker Studio

**理由**:
1. **リース再取得日の影響を正しく反映**
   - 期首時点でリース再取得前なら、取得原価・減価償却累計額は0
   - 期末時点でリース再取得後なら、通常通り計算

2. **会計処理として正確**
   - リース期間中は自社資産ではないため、簿価計算の対象外
   - リース再取得後に自社資産として認識し、簿価計算を開始

3. **Pythonの実装は簡易版**
   - リース再取得日の影響を一部省略している
   - Looker Studioの方が実務に即した完全版

---

## 🔧 SQL実装時の注意点

### 1. リース再取得日の月初化

Looker Studio: `DATETIME_TRUNC(リース資産再取得日, MONTH)`

BigQuery SQL:
```sql
DATE_TRUNC(lease_reacquisition_date, MONTH)
```

### 2. NARY_MAX/NARY_MIN関数

Looker Studio: `NARY_MAX(a, b)` / `NARY_MIN(a, b)`

BigQuery SQL:
```sql
GREATEST(a, b)  -- NARY_MAX相当
LEAST(a, b)     -- NARY_MIN相当
```

### 3. レベシェア判定

Looker Studio:
```sql
REGEXP_CONTAINS(supplier_name, 'レベシェア')
and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')
```

BigQuery SQL（そのまま使用可）

---

## 📝 次のステップ

1. ✅ **Looker Studioのロジックを採用**してSQL実装
2. ✅ **リース再取得日の影響**を全項目で考慮
3. ✅ **償却α/β/γ**の計算ロジックをそのまま移植
4. ✅ **減損損失**の計算でもリース再取得日を考慮

---

## 付録: 完全一致している項目

以下の項目はLooker StudioとPythonで完全に一致しています:

- 償却α（`calculate_shokyaku_alpha`）
- 償却β（`calculate_shokyaku_beta`）
- 償却γ（`calculate_shokyaku_gamma`）
- 増加取得原価（`calculate_acquisition_cost_increase`）
- 減少取得原価（`calculate_acquisition_cost_decrease`）
- 償却償却月数（`calculate_amortization_months_shokyaku`）
- 増加償却月数（`calculate_amortization_months_increase`）
- 減少償却月数（`calculate_amortization_months_decrease`）
- 減少減価償却累計額（`calculate_accumulated_depreciation_decrease`）
- 減少減損損失累計額（`calculate_new_impairment_loss_decrease`）
- 期首簿価（`calculate_opening_book_value`）
- 増加簿価（`calculate_increase_book_value`）
- 減少簿価（`calculate_decrease_book_value`）
- 期末簿価（`calculate_closing_book_value`）

これらの項目はLooker StudioのコードをそのままSQL化すればOKです。
