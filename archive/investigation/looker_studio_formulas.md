# Looker Studio 簿価計算フォーミュラ一覧

**作成日**: 2025-12-25
**目的**: Looker Studioで実装している簿価計算の各項目のコードを整理し、Pythonコードおよび想定実装との差異を確認

---

## 📝 使用方法

各項目の「**コード**」セクションに、Looker Studioの計算フィールドのコードを貼り付けてください。

---

## 基礎データ項目

### 契約識別コード

**説明**: サプライヤー名から契約識別コード（4桁）を抽出

**コード**:
```sql
<!-- ここにLooker Studioのコードを貼り付け -->

```

**備考**:


---

### 初回出荷日

**説明**: サプライヤー名から初回出荷日を計算、またはデフォルト値を使用

**コード**:
```sql
<!-- ここにLooker Studioのコードを貼り付け -->

```

**備考**:


---

### リース再取得日

**説明**: サプライヤー名から借手リース終了後の再取得日を計算

**コード**:
```sql
<!-- ここにLooker Studioのコードを貼り付け -->

```

**備考**:


---

## 計算項目: 取得原価関連

### 実際の取得原価

**説明**: cost + overhead_cost - discount

**コード**:
```sql
<!-- ここにLooker Studioのコードを貼り付け -->

```

**備考**:


---

### 期首取得原価

**説明**: 期首時点で保有している資産の取得原価

**Pythonでの関数**: `calculate_initial_cost()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN inspected_at is null THEN 0
	WHEN 計算基準日(期首日) <= inspected_at THEN 0
    WHEN 計算基準日(期首日) <= DATETIME_TRUNC(リース資産再取得日, MONTH) THEN 0
	WHEN impossibled_at < 計算基準日(期首日) or lease_first_shipped_at < 計算基準日(期首日) THEN 0
    ELSE 取得原価
END

```

**備考**:


---

### 増加取得原価

**説明**: 期中に新規入庫またはリース再取得した資産の取得原価

**Pythonでの関数**: `calculate_acquisition_cost_increase()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
    WHEN  (計算基準日(期首日) <= リース資産再取得日 and リース資産再取得日 <= 計算基準日(期末日) and (impossibled_at is null or impossibled_at>=DATETIME_TRUNC(リース資産再取得日, MONTH)) and (lease_first_shipped_at is null or  lease_first_shipped_at>=DATETIME_TRUNC(リース資産再取得日, MONTH))) Then 取得原価
    WHEN (計算基準日(期首日) <= inspected_at and inspected_at <= 計算基準日(期末日)) then  取得原価
    ELSE 0
END

```

**備考**:


---

### 減少取得原価

**説明**: 期中に売却・除却・貸手リースに転換した資産の取得原価

**Pythonでの関数**: `calculate_acquisition_cost_decrease()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN impossibled_at <計算基準日(期首日) or lease_first_shipped_at <計算基準日(期首日) THEN 0
	WHEN impossibled_at <DATETIME_TRUNC(リース資産再取得日,month) or lease_first_shipped_at <DATETIME_TRUNC(リース資産再取得日,month) THEN 0
    WHEN lease_first_shipped_at > 計算基準日(期末日) THEN 0
    WHEN lease_first_shipped_at is null and (impossibled_at is null or impossibled_at >計算基準日(期末日)) THEN 0
    ELSE 取得原価

```

**備考**:


---

### 期末取得原価

**説明**: 期末時点で保有している資産の取得原価

**Pythonでの関数**: `calculate_acquisition_cost_kimatsu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN inspected_at is null THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日)　THEN 0
	WHEN inspected_at > 計算基準日(期末日) or impossibled_at <= 計算基準日(期末日) or lease_first_shipped_at <= 計算基準日(期末日) THEN 0
    ELSE 取得原価
END

```

**備考**:


---

## 計算項目: 償却期間（α/β/γ）

### 償却α

**説明**: 期首時点での償却済み月数

**Pythonでの関数**: `calculate_shokyaku_alpha()`

**コード**:
```sql
CASE
	WHEN 初回出荷日 >=計算基準日(期首日) or 初回出荷日 is NULL THEN 0
    WHEN impairment_date < 計算基準日(期首日) AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL) OR impossibled_at > impairment_date OR lease_first_shipped_at > impairment_date)
    	THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日)) + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    WHEN lease_first_shipped_at < 計算基準日(期首日)
    	THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日)) + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    WHEN impossibled_at < 計算基準日(期首日) and classification_of_impossibility = "庫内紛失／棚卸差異"
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)
    WHEN impossibled_at < 計算基準日(期首日) 
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日), 0)     
    ELSE 12*(YEAR(計算基準日(期首日)) - YEAR(初回出荷日)) + MONTH(計算基準日(期首日)) - MONTH(初回出荷日) 
END

```

**備考**:


---

### 償却β

**説明**: 期末時点での償却済み月数

**Pythonでの関数**: `calculate_shokyaku_beta()`

**コード**:
```sql
CASE
	WHEN 初回出荷日 > 計算基準日(期末日) or 初回出荷日 is NULL THEN 0
    WHEN impairment_date <= 計算基準日(期末日) AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL) OR impossibled_at > impairment_date OR lease_first_shipped_at > impairment_date)
    	THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日)) + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    WHEN lease_first_shipped_at <= 計算基準日(期末日)
    	THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日)) + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    WHEN impossibled_at <= 計算基準日(期末日) and classification_of_impossibility = "庫内紛失／棚卸差異"
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)        
    WHEN impossibled_at <= 計算基準日(期末日)
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日), 0)
    ELSE 12*(YEAR(計算基準日(期末日)) - YEAR(初回出荷日)) + MONTH(計算基準日(期末日)) - MONTH(初回出荷日) + 1
END

```

**備考**:


---

### 償却γ

**説明**: リース再取得時点の償却済み月数

**Pythonでの関数**: `calculate_shokyaku_gamma()`

**コード**:
```sql
CASE
　　WHEN リース資産再取得日 is null THEN 0
    WHEN impairment_date < DATETIME_TRUNC(リース資産再取得日, MONTH) AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL) OR impossibled_at > impairment_date OR lease_first_shipped_at > impairment_date)
    	THEN NARY_MAX(12*(YEAR(impairment_date) - YEAR(初回出荷日)) + MONTH(impairment_date) - MONTH(初回出荷日) + 1, 0)
    WHEN lease_first_shipped_at <DATETIME_TRUNC(リース資産再取得日, MONTH)
    	THEN NARY_MAX(12*(YEAR(lease_first_shipped_at) - YEAR(初回出荷日)) + MONTH(lease_first_shipped_at) - MONTH(初回出荷日), 0)
    WHEN impossibled_at < DATETIME_TRUNC(リース資産再取得日, MONTH) and classification_of_impossibility = "庫内紛失／棚卸差異"
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日) + 1, 0)
    WHEN impossibled_at < DATETIME_TRUNC(リース資産再取得日, MONTH)
    	THEN NARY_MAX(12*(YEAR(impossibled_at) - YEAR(初回出荷日)) + MONTH(impossibled_at) - MONTH(初回出荷日), 0)     
    ELSE 12*(YEAR(リース資産再取得日) - YEAR(初回出荷日)) + MONTH(リース資産再取得日) - MONTH(初回出荷日) 
END

```

**備考**:


---

## 計算項目: 償却月数

### 期首償却月数

**説明**: 期首時点の償却月数（耐用年数との比較）

**Pythonでの関数**: `calculate_amortization_months_kishu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN classification_of_impossibility = "レベシェア" THEN 0
    WHEN inspected_at >= 計算基準日(期首日) THEN 0
    WHEN lease_first_shipped_at <計算基準日(期首日) OR impossibled_at < 計算基準日(期首日) THEN 0
    ELSE NARY_MIN(償却α, 12*depreciation_period)
END

```

**備考**:


---

### 償却償却月数（期中償却月数）

**説明**: 期中に償却した月数

**Pythonでの関数**: `calculate_amortization_months_shokyaku()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
    WHEN inspected_at > 計算基準日(期末日) THEN 0
　　WHEN DATETIME_TRUNC(リース資産再取得日,month)> 計算基準日(期末日) THEN 0
  　WHEN DATETIME_TRUNC(リース資産再取得日,month)>= 計算基準日(期首日) THEN NARY_MIN(depreciation_period*12, 償却β) - NARY_MIN(depreciation_period*12, 償却γ)
    ELSE NARY_MIN(depreciation_period*12, 償却β) - NARY_MIN(depreciation_period*12, 償却α)
END

```

**備考**:


---

### 増加償却月数

**説明**: リース再取得時の償却済み月数

**Pythonでの関数**: `calculate_amortization_months_increase()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN DATETIME_TRUNC(リース資産再取得日,month)<計算基準日(期首日) or DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日) OR impossibled_at <DATETIME_TRUNC(リース資産再取得日,month) OR lease_first_shipped_at <DATETIME_TRUNC(リース資産再取得日,month) THEN 0
    ELSE NARY_MIN(償却γ, depreciation_period*12)
END

```

**備考**:


---

### 減少償却月数

**説明**: 除却・貸手リース転換時の償却済み月数

**Pythonでの関数**: `calculate_amortization_months_decrease()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN classification_of_impossibility = "レベシェア品" THEN 0
	WHEN impossibled_at <計算基準日(期首日) OR lease_first_shipped_at <計算基準日(期首日) THEN 0
	WHEN impossibled_at <DATETIME_TRUNC(リース資産再取得日,month) or lease_first_shipped_at <DATETIME_TRUNC(リース資産再取得日,month) THEN 0
    WHEN lease_first_shipped_at > 計算基準日(期末日) THEN 0
    WHEN lease_first_shipped_at is null and (impossibled_at is null or impossibled_at >計算基準日(期末日)) THEN 0
    ELSE NARY_MIN(償却β, depreciation_period*12)
END

```

**備考**:


---

### 期末償却月数

**説明**: 期末時点の償却月数（耐用年数との比較）

**Pythonでの関数**: `calculate_shokyaku_months_kimatsu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN classification_of_impossibility = "レベシェア" THEN 0
    WHEN inspected_at > 計算基準日(期末日) THEN 0
    WHEN lease_first_shipped_at <= 計算基準日(期末日) or impossibled_at <= 計算基準日(期末日) THEN 0
    ELSE NARY_MIN(償却β, 12*depreciation_period)
END

```

**備考**:


---

## 計算項目: 減価償却累計額

### 月次償却額

**説明**: 月ごとの減価償却額

**計算式**: 取得原価 / (耐用年数 × 12)

**コード**:
```sql
<!-- ここにLooker Studioのコードを貼り付け -->

```

**備考**:


---

### 期首減価償却累計額

**説明**: 期首時点の減価償却累計額

**Pythonでの関数**: `calculate_accumulated_depreciation_kishu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN 償却月数(期首) = 0 THEN 0
    WHEN 計算基準日(期首日) <= DATETIME_TRUNC(リース資産再取得日, MONTH) THEN 0
    WHEN 取得原価 < NARY_MIN(depreciation_period*12, 償却α) or depreciation_period*12 <= 償却α THEN 取得原価
    ELSE 月次償却額(会計) *償却月数(期首) 
END

```

**備考**:


---

### 増加減価償却累計額

**説明**: リース再取得時の減価償却累計額

**Pythonでの関数**: `calculate_accumulated_depreciation_increase()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN 償却月数(増加) = 0 THEN 0
    WHEN 取得原価 < NARY_MIN(depreciation_period*12, 償却γ) or depreciation_period*12 = 償却月数(増加) THEN 取得原価
    ELSE 月次償却額(会計) *償却月数(増加)
END

```

**備考**:


---

### 減少減価償却累計額

**説明**: 除却・貸手リース転換時の減価償却累計額

**Pythonでの関数**: `calculate_accumulated_depreciation_decrease()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN 償却月数(減少) = 0 THEN 0
    WHEN 取得原価 < NARY_MIN(depreciation_period*12, 償却β) or depreciation_period*12 = 償却月数(減少) THEN 取得原価
    ELSE 月次償却額(会計) *償却月数(減少)
END

```

**備考**:


---

### 期中減価償却費

**説明**: 期中に計上する減価償却費

**Pythonでの関数**: `calculate_interim_depreciation_expense()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN 償却月数(償却) = 0 THEN 0
    WHEN 取得原価 <= depreciation_period*12 and 取得原価 > 償却α and 取得原価 < 償却β THEN 取得原価 - 償却α
    WHEN 取得原価 < NARY_MIN(償却α, depreciation_period*12) THEN 0
    WHEN depreciation_period*12 <= 償却β THEN 取得原価 - 月次償却額(会計)*(depreciation_period*12 - 1) + 月次償却額(会計)*(償却月数(償却) - 1)
    ELSE 月次償却額(会計) *償却月数(償却)
END

```

**備考**:


---

### 期末減価償却累計額

**説明**: 期末時点の減価償却累計額

**Pythonでの関数**: `calculate_accumulated_depreciation_kimatsu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN 償却月数(期末) = 0 THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日)　THEN 0
    WHEN 取得原価 < NARY_MIN(depreciation_period*12, 償却β) or depreciation_period*12 <= 償却β THEN 取得原価
    ELSE 月次償却額(会計) *償却月数(期末)
END

```

**備考**:


---

## 計算項目: 減損損失累計額

### 期首減損損失累計額

**説明**: 期首時点の減損損失累計額

**Pythonでの関数**: `calculate_new_impairment_loss_kishu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
ELSE IF(impairment_date < 計算基準日(期首日), 取得原価(期首) - 減価償却累計額(期首), 0) END

```

**備考**:


---

### 増加減損損失累計額

**説明**: 期中増加分の減損損失累計額

**Pythonでの関数**: `calculate_new_impairment_loss_increase()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日)　THEN 0
	WHEN inspected_at <= 計算基準日(期末日) and 計算基準日(期首日) <= impairment_date and impairment_date <= 計算基準日(期末日) THEN 簿価(期首)+簿価(増加) - 減価償却累計額(償却)
    ELSE 0
END

```

**備考**:


---

### 減少減損損失累計額

**説明**: 期中減少分の減損損失累計額

**Pythonでの関数**: `calculate_new_impairment_loss_decrease()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
	WHEN impairment_date > 計算基準日(期末日) THEN 0
    WHEN impairment_date is null THEN 0
    ELSE 取得原価(減少) - 減価償却累計額(減少)
END

```

**備考**:


---

### 期末減損損失累計額

**説明**: 期末時点の減損損失累計額

**Pythonでの関数**: `calculate_new_impairment_loss_kimatsu()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日)　THEN 0
	WHEN impairment_date <= 計算基準日(期末日) THEN 取得原価(期末) - 減価償却累計額(期末)
    ELSE 0
END

```

**備考**:


---

### 期中減損損失

**説明**: 期中に計上する減損損失

**Pythonでの関数**: `calculate_new_interim_impairment_loss()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
    WHEN DATETIME_TRUNC(リース資産再取得日,month)>計算基準日(期末日)　THEN 0
	WHEN inspected_at <= 計算基準日(期末日) and 計算基準日(期首日) <= impairment_date and impairment_date <= 計算基準日(期末日) THEN 簿価(期首)+簿価(増加) - 減価償却累計額(償却)
    ELSE 0
END

```

**備考**:


---

## 計算項目: 簿価

### 期首簿価

**説明**: 期首時点の簿価（取得原価 - 減価償却累計額 - 減損損失累計額）

**Pythonでの関数**: `calculate_opening_book_value()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
ELSE 取得原価(期首) - 減価償却累計額(期首) - 減損損失累計額(期首) END

```

**備考**:


---

### 増加簿価

**説明**: 期中増加分の簿価

**Pythonでの関数**: `calculate_increase_book_value()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
ELSE 取得原価(増加) - 減価償却累計額(増加) - 減損損失累計額(増加) END

```

**備考**:


---

### 減少簿価

**説明**: 期中減少分の簿価

**Pythonでの関数**: `calculate_decrease_book_value()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
ELSE 取得原価(減少) - 減価償却累計額(減少) - 減損損失累計額(減少) END

```

**備考**:


---

### 期末簿価

**説明**: 期末時点の簿価（最終的な帳簿価額）

**Pythonでの関数**: `calculate_closing_book_value()`

**コード**:
```sql
CASE
	WHEN (REGEXP_CONTAINS(supplier_name, 'レベシェア') and not REGEXP_CONTAINS(supplier_name, 'リース・レベシェア')) or REGEXP_CONTAINS(supplier_name, "法人小物管理用") or sample THEN 0
ELSE 取得原価(期末) - 減価償却累計額(期末) - 減損損失累計額(期末) END

```

**備考**:


---

## その他の計算項目

### 資産分類

**説明**: サプライヤー名とsampleフラグから資産分類を判定

**Pythonでの関数**: `classify_asset()`

**コード**:
```sql
CASE
	WHEN REGEXP_CONTAINS(supplier_name, "リース・レベシェア") THEN "リース資産(借手リース)"
	WHEN REGEXP_CONTAINS(supplier_name, "レベシェア") THEN "レベシェア品"
	WHEN REGEXP_CONTAINS(supplier_name, "法人小物管理用") THEN "小物等"
    WHEN sample THEN "サンプル品"
    ELSE "賃貸用固定資産"
END
```

**備考**:


---

### 会計ステータス

**説明**: 期末時点の会計上の扱いを判定

**Pythonでの関数**: `determine_accounting_status()`

**コード**:
```sql
CASE
    WHEN 資産分類 =　"レベシェア品" THEN "計上外(レベシェア)"
    WHEN 資産分類 =　"小物等" THEN "仕入高(小物等)"
    WHEN 資産分類　= "サンプル品" THEN "研究開発費(サンプル品)"
	WHEN inspected_at >計算基準日(期末日) or inspected_at is NULL THEN "計上外(入庫検品前)"
    WHEN impossibled_at　<= 計算基準日(期末日) and classification_of_impossibility IN ("売却（顧客）", "売却（法人案件）", "売却（EC）") THEN "仕入高(売却)"
    WHEN impossibled_at　<= 計算基準日(期末日) and classification_of_impossibility IN ("貸倒/所有権放棄", "貸倒") THEN "雑費(除却)"
    WHEN impossibled_at　<= 計算基準日(期末日) THEN "家具廃棄損(除却)"
    WHEN lease_first_shipped_at　<= 計算基準日(期末日) THEN "リース債権(貸手リース)"
    ELSE 資産分類
END

```

**備考**:


---

## 差異確認チェックリスト

コード貼り付け完了後、以下の観点で差異を確認します：

- [ ] リース再取得日の計算ロジック
- [ ] 期首取得原価でのリース再取得日の影響
- [ ] 増加取得原価でのリース再取得日の影響
- [ ] 償却α/β/γの計算ロジック
- [ ] 償却償却月数でのリース再取得日の影響（β-γ vs β-α）
- [ ] 増加償却月数でのリース再取得日の影響
- [ ] 減価償却累計額の計算ロジック
- [ ] 減損損失の計算ロジック
- [ ] 簿価の計算ロジック
- [ ] Pythonコードとの主要な差異

---

## 確認完了後の対応

1. **差異の分析**: Looker Studio vs Python vs 想定実装
2. **正しいロジックの確定**: どのロジックを採用すべきか判断
3. **SQL実装への反映**: 確定したロジックでBigQuery SQLを実装

---

## 備考欄

<!-- コード貼り付け作業中のメモや気づきを記入 -->
