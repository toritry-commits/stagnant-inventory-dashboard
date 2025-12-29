# Pythonコード 簿価計算ロジック分析

**作成日**: 2025-12-25
**分析対象**: `python_book_value_logic.py`
**目的**: BigQuery SQLへの移植のための理解と必要データの洗い出し

---

## 目次

1. [全体構成](#全体構成)
2. [計算の流れ](#計算の流れ)
3. [主要計算関数の詳細](#主要計算関数の詳細)
4. [必要なデータ項目](#必要なデータ項目)
5. [SQL移植の方針](#sql移植の方針)

---

## 全体構成

### アプリケーション概要

- **目的**: 固定資産台帳の簿価計算（減価償却・減損損失を含む）
- **入力**: CSVファイル（在庫データ）
- **出力**: 計算済み固定資産台帳CSV
- **処理期間**: 期首日〜期末日を指定して計算

### 計算する主要項目

```
【取得原価】
- 期首取得原価
- 増加取得原価（期中入庫）
- 減少取得原価（期中売却・除却）
- 期末取得原価

【減価償却】
- 償却α/β/γ（償却期間計算用）
- 期首/増加/減少/期末 減価償却累計額
- 期中減価償却費

【減損損失】
- 期首/増加/減少/期末 減損損失累計額
- 期中減損損失

【簿価】
- 期首簿価 = 期首取得原価 - 期首減価償却累計額 - 期首減損損失累計額
- 増加簿価 = 増加取得原価 - 増加減価償却累計額 - 増加減損損失累計額
- 減少簿価 = 減少取得原価 - 減少減価償却累計額 - 減少減損損失累計額
- 期末簿価 = 期末取得原価 - 期末減価償却累計額 - 期末減損損失累計額
```

---

## 計算の流れ

### ステップ1: 前処理・分類

```python
# 1-1. 資産分類の決定
classify_asset(row) → "賃貸用固定資産" / "リース資産(借手リース)" / "レベシェア品" / "サンプル品" / "小物等"

# 1-2. 会計ステータスの決定
determine_accounting_status(row, end_date) →
  "賃貸用固定資産" / "リース資産(借手リース)" / "仕入高(売却)" / "家具廃棄損(除却)" /
  "計上外(入庫検品前)" / "計上外(レベシェア)" / "研究開発費(サンプル品)" など

# 1-3. 特殊日付の計算
calculate_lease_reacquisition_date(supplier_name) → リース再取得日（特定パターンで判定）
calculate_first_shipped_at_calculated(row) → 供与開始日（初回出荷日）
```

**ポイント**:
- サプライヤー名で資産種別を判定（"リース・レベシェア"、"レベシェア"、"法人小物管理用"）
- 除外対象: レベシェア品、小物等、サンプル品（これらは簿価計算対象外）

---

### ステップ2: 取得原価の計算

```python
# 2-1. 期首取得原価
calculate_initial_cost(row, start_date)
→ 期首時点で既に存在していた資産の取得原価

条件:
- 入庫検品完了日が期首日より前
- リース再取得日が期首日より前（該当する場合）
- 除売却日・貸手リース開始日が期首日以降（まだ存在している）

# 2-2. 増加取得原価
calculate_acquisition_cost_increase(row, start_date, end_date)
→ 期中に新規入庫またはリース再取得した資産の取得原価

条件:
- 入庫検品完了日が期首日〜期末日の間
- またはリース再取得日が期首日〜期末日の間

# 2-3. 減少取得原価
calculate_acquisition_cost_decrease(row, start_date, end_date)
→ 期中に売却・除却・貸手リースに転換した資産の取得原価

条件:
- 除売却日が期首日〜期末日の間
- または貸手リース開始日が期首日〜期末日の間

# 2-4. 期末取得原価
calculate_acquisition_cost_kimatsu(row, end_date)
→ 期末時点で保有している資産の取得原価
= 期首取得原価 + 増加取得原価 - 減少取得原価

条件:
- 会計ステータスが「賃貸用固定資産」または「リース資産(借手リース)」のみ
```

**ポイント**:
- 取得原価の移動（増加・減少）を追跡
- リース再取得（借手リース終了後の再取得）のタイミングも考慮

---

### ステップ3: 償却期間の計算（α/β/γ）

これらは減価償却累計額の計算に必要な「何ヶ月分償却するか」を決定する中間値。

#### 償却α（期首時点での償却済み月数）

```python
calculate_shokyaku_alpha(row, start_date)
→ 初回出荷日から期首日までの経過月数

特殊ケース:
- 減損損失日が期首より前 → 減損時点までの月数+1
- 貸手リース開始日が期首より前 → 貸手リース開始時点までの月数
- 除売却日が期首より前 → 除売却時点までの月数（棚卸差異の場合+1）
```

#### 償却β（期末時点での償却済み月数）

```python
calculate_shokyaku_beta(row, end_date)
→ 初回出荷日から期末日までの経過月数+1

特殊ケース:
- 減損損失日が期末以前 → 減損時点までの月数+1
- 貸手リース開始日が期末以前 → 貸手リース開始時点までの月数
- 除売却日が期末以前 → 除売却時点までの月数（棚卸差異の場合+1）
```

#### 償却γ（リース再取得時点での償却済み月数）

```python
calculate_shokyaku_gamma(row)
→ 初回出荷日からリース再取得日までの経過月数

※ リース再取得がない場合は0
```

**ポイント**:
- `+1` がつくケース: 減損損失、棚卸差異での除売却（当月分も償却済みとみなす）
- `+1` がつかないケース: 通常の貸手リース転換、通常の除売却（当月分は未償却）

---

### ステップ4: 償却月数の計算

```python
# 4-1. 期首償却月数
calculate_amortization_months_kishu(row, start_date)
→ min(償却α, 耐用年数×12)

# 4-2. 償却償却月数（期中に償却した月数）
calculate_amortization_months_shokyaku(row, start_date, end_date)
→ リース再取得がある場合: min(償却β, 耐用年数×12) - min(償却γ, 耐用年数×12)
   リース再取得がない場合: min(償却β, 耐用年数×12) - min(償却α, 耐用年数×12)

# 4-3. 増加償却月数（リース再取得時の償却済み月数）
calculate_amortization_months_increase(row, start_date, end_date)
→ リース再取得日が期首〜期末の間にある場合: min(償却γ, 耐用年数×12)

# 4-4. 減少償却月数（除却・貸手リース転換時の償却済み月数）
calculate_amortization_months_decrease(row, start_date, end_date)
→ 除売却日または貸手リース開始日が期首〜期末の間にある場合: min(償却β, 耐用年数×12)

# 4-5. 期末償却月数
calculate_shokyaku_months_kimatsu(row, end_date)
→ min(償却β, 耐用年数×12)
※ 会計ステータスが「賃貸用固定資産」または「リース資産(借手リース)」のみ
```

**ポイント**:
- 耐用年数を超えて償却しない（min関数で制限）
- リース再取得時は、過去の償却月数を引き継ぐ

---

### ステップ5: 減価償却累計額の計算

```python
# 月次償却額（入力データに含まれる）
月次償却額 = 取得原価 / (耐用年数 × 12)

# 5-1. 期首減価償却累計額
calculate_accumulated_depreciation_kishu(row, start_date)
→ 通常: 月次償却額 × 期首償却月数
   特殊: 耐用年数到達済みまたは取得原価<償却月数の場合 → 取得原価

# 5-2. 増加減価償却累計額
calculate_accumulated_depreciation_increase(row)
→ 通常: 月次償却額 × 増加償却月数
   特殊: 耐用年数到達済みまたは取得原価<償却月数の場合 → 取得原価

# 5-3. 減少減価償却累計額
calculate_accumulated_depreciation_decrease(row)
→ 通常: 月次償却額 × 減少償却月数
   特殊: 耐用年数到達済みまたは取得原価<償却月数の場合 → 取得原価

# 5-4. 期末減価償却累計額
calculate_accumulated_depreciation_kimatsu(row, end_date)
→ 通常: 月次償却額 × 期末償却月数
   特殊: 耐用年数到達済みまたは取得原価<償却月数の場合 → 取得原価

# 5-5. 期中減価償却費
calculate_interim_depreciation_expense(row)
→ 複雑なロジックで、期中に計上する減価償却費を計算
   基本: 月次償却額 × 償却償却月数
   特殊ケースで調整あり
```

**ポイント**:
- 減価償却累計額は取得原価を超えない
- 耐用年数到達済みの場合、簿価=0になるように調整

---

### ステップ6: 減損損失累計額の計算

```python
# 6-1. 期首減損損失累計額
calculate_new_impairment_loss_kishu(row, start_date)
→ 減損損失日が期首日より前の場合: 期首簿価（取得原価-減価償却累計額）

# 6-2. 増加減損損失累計額
calculate_new_impairment_loss_increase(row, start_date, end_date)
→ 減損損失日が増加取得原価の取得後〜期末の間にある場合: 増加簿価（増加取得原価-増加減価償却累計額）

# 6-3. 減少減損損失累計額
calculate_new_impairment_loss_decrease(row, end_date)
→ 減損損失日が期末以前で、かつ減少取得原価がある場合: 減少簿価（減少取得原価-減少減価償却累計額）

# 6-4. 期末減損損失累計額
calculate_new_impairment_loss_kimatsu(row, end_date)
→ 減損損失日が期末日以前の場合: 期末簿価（期末取得原価-期末減価償却累計額）

# 6-5. 期中減損損失
calculate_new_interim_impairment_loss(row, start_date, end_date)
→ 減損損失日が期首〜期末の間にある場合: max(減少減損損失累計額, 期末減損損失累計額)
```

**ポイント**:
- 減損損失は「回収不能になった簿価」を一括で損失計上する
- 減損後は簿価=0になる（取得原価 = 減価償却累計額 + 減損損失累計額）

---

### ステップ7: 簿価の計算（最終結果）

```python
# 7-1. 期首簿価
calculate_opening_book_value(row)
= 期首取得原価 - 期首減価償却累計額 - 期首減損損失累計額

# 7-2. 増加簿価
calculate_increase_book_value(row)
= 増加取得原価 - 増加減価償却累計額 - 増加減損損失累計額

# 7-3. 減少簿価
calculate_decrease_book_value(row)
= 減少取得原価 - 減少減価償却累計額 - 減少減損損失累計額

# 7-4. 期末簿価
calculate_closing_book_value(row)
= 期末取得原価 - 期末減価償却累計額 - 期末減損損失累計額

条件:
- 会計ステータスが「賃貸用固定資産」または「リース資産(借手リース)」のみ
- 負の値は0にする（max(0, 計算値)）
```

**ポイント**:
- 簿価 = 取得原価から減価償却・減損損失を差し引いた現在価値
- 会計上、「賃貸用固定資産」「リース資産」のみが簿価を持つ

---

## 主要計算関数の詳細

### calculate_shokyaku_alpha/beta/gamma

**目的**: 減価償却の計算基準となる「償却済み月数」を決定

**複雑な条件分岐**:

| 状態 | 償却終了日 | 備考 |
|------|-----------|------|
| 通常使用中 | 期首日/期末日 | 初回出荷日からの経過月数 |
| 減損済み | 減損損失日 | 減損時点で償却停止 |
| 貸手リース転換 | 貸手リース開始日 | 自社資産→賃貸債権に変更 |
| 除売却 | 除売却日 | 棚卸差異の場合は+1月 |

**SQL移植のポイント**:
```sql
CASE
  WHEN 減損損失日 IS NOT NULL AND 減損損失日 < 期首日 THEN
    DATE_DIFF(減損損失日, 初回出荷日, MONTH) + 1
  WHEN 貸手リース開始日 IS NOT NULL AND 貸手リース開始日 < 期首日 THEN
    DATE_DIFF(貸手リース開始日, 初回出荷日, MONTH)
  WHEN 除売却日 IS NOT NULL AND 除売却日 < 期首日 THEN
    CASE WHEN 破損紛失分類 = '庫内紛失／棚卸差異'
         THEN DATE_DIFF(除売却日, 初回出荷日, MONTH) + 1
         ELSE DATE_DIFF(除売却日, 初回出荷日, MONTH)
    END
  ELSE DATE_DIFF(期首日, 初回出荷日, MONTH)
END
```

---

### calculate_initial_cost/acquisition_cost_increase/decrease

**目的**: 期首・期中増加・期中減少の取得原価を計算

**除外条件（共通）**:
```python
# 以下は簿価計算対象外
- サプライヤー名に「レベシェア」（「リース・レベシェア」を除く）
- サプライヤー名に「法人小物管理用」
- sample = True
```

**期首取得原価の条件**:
```python
1. 入庫検品完了日が期首日より前
2. リース再取得日が期首日より前（該当する場合）
3. 除売却日が期首日以降（まだ除却されていない）
4. 貸手リース開始日が期首日以降（まだ自社資産）
```

**増加取得原価の条件**:
```python
1. 入庫検品完了日が期首日〜期末日の間
   OR
2. リース再取得日が期首日〜期末日の間
   かつ 除売却日・貸手リース開始日がリース再取得日以降
```

**減少取得原価の条件**:
```python
1. 除売却日が期首日〜期末日の間
   OR
2. 貸手リース開始日が期首日〜期末日の間
```

---

### calculate_accumulated_depreciation_xxx

**目的**: 減価償却累計額を計算

**基本式**:
```
減価償却累計額 = 月次償却額 × 償却月数
```

**特殊ケース（上限チェック）**:
```python
# ケース1: 耐用年数到達済み
if 耐用年数×12 <= 償却月数:
    return 取得原価  # 全額償却

# ケース2: 取得原価が償却月数より小さい
if 取得原価 < 償却月数:
    return 取得原価  # 全額償却

# ケース3: 通常
return 月次償却額 × 償却月数
```

**SQL移植のポイント**:
```sql
CASE
  WHEN 耐用年数 * 12 <= 償却月数 THEN 取得原価
  WHEN 取得原価 < 償却月数 THEN 取得原価
  ELSE 月次償却額 * 償却月数
END
```

---

### calculate_new_impairment_loss_xxx

**目的**: 減損損失累計額を計算

**期首減損損失累計額**:
```python
if 減損損失日 < 期首日:
    return max(0, 期首取得原価 - 期首減価償却累計額)
else:
    return 0
```

**期末減損損失累計額**:
```python
if 減損損失日 <= 期末日:
    return max(0, 期末取得原価 - 期末減価償却累計額)
else:
    return 0
```

**期中減損損失**:
```python
if 期首日 <= 減損損失日 <= 期末日:
    return max(減少減損損失累計額, 期末減損損失累計額)
else:
    return 0
```

**ポイント**:
- 減損損失 = 減損時点の簿価（取得原価-減価償却累計額）
- 減損後は簿価=0になる

---

### calculate_opening/closing_book_value

**目的**: 簿価を計算（最終的な帳簿価額）

**計算式**:
```python
期首簿価 = 期首取得原価 - 期首減価償却累計額 - 期首減損損失累計額
期末簿価 = 期末取得原価 - 期末減価償却累計額 - 期末減損損失累計額

# 期末簿価は会計ステータスによる制限あり
if 会計ステータス not in ["賃貸用固定資産", "リース資産(借手リース)"]:
    期末簿価 = 0
```

**SQL移植のポイント**:
```sql
-- 期首簿価
期首取得原価 - 期首減価償却累計額 - 期首減損損失累計額 AS 期首簿価,

-- 期末簿価
CASE
  WHEN 会計ステータス IN ('賃貸用固定資産', 'リース資産(借手リース)')
  THEN GREATEST(0, 期末取得原価 - 期末減価償却累計額 - 期末減損損失累計額)
  ELSE 0
END AS 期末簿価
```

---

## 必要なデータ項目

### 必須項目（BigQueryに存在すべきデータ）

| データ項目 | 説明 | Pythonでの列名 | BigQueryテーブル候補 | データ型 |
|-----------|------|---------------|-------------------|---------|
| 在庫ID | 在庫の一意識別子 | `在庫id` | `lake.stock.id` | INT64 |
| パーツID | パーツの一意識別子 | `パーツid` | `lake.stock.part_id` | INT64 |
| パーツ名 | パーツ名称 | `パーツ名` | `lake.part.name` | STRING |
| 取得原価 | 購入時の価格 | `取得原価` | `lake.stock.cost` | FLOAT64 |
| 耐用年数 | 減価償却の期間（年） | `耐用年数` | `lake.part.useful_life_years` | INT64 |
| 入庫検品完了日 | 入庫して検品が完了した日 | `入庫検品完了日` | `lake.stock.inspected_at` | DATE |
| 除売却日 | 除却または売却した日 | `除売却日` | `lake.stock.impossibility_at` | DATE |
| 破損紛失分類 | 除却理由の分類 | `破損紛失分類` | `lake.stock.classification_of_impossibility` | STRING |
| サプライヤー名 | 仕入先名称 | `サプライヤー名` | `lake.supplier.name` | STRING |
| 減損損失日 | 減損損失を計上した日 | `減損損失日` | `lake.stock.impairment_date` | DATE |

### 計算で導出可能な項目（Pythonで計算しているもの）

| データ項目 | 説明 | 計算方法 |
|-----------|------|---------|
| 月次償却額 | 月ごとの償却額 | `取得原価 / (耐用年数 × 12)` |
| 初回出荷日 | 供与開始日（最初に出荷した日） | サプライヤー名のパターンで判定、またはCSV入力 |
| 資産分類 | 資産の分類（賃貸用固定資産等） | サプライヤー名とsampleフラグで判定 |
| 会計ステータス | 期末時点の会計上の扱い | 資産分類・入庫日・除売却日・貸手リース開始日で判定 |
| リース再取得日 | 借手リース終了後の再取得日 | サプライヤー名のパターンで判定 |

### オプション項目（特定のビジネスケースで使用）

| データ項目 | 説明 | Pythonでの列名 | 使用ケース |
|-----------|------|---------------|-----------|
| sample | サンプル品フラグ | `sample` | サンプル品は簿価計算対象外 |
| 貸手リース開始日 | 自社資産→賃貸債権に転換した日 | `貸手リース開始日` | リース債権への転換時 |
| 売却案件名 | 売却先の案件名 | `売却案件名` | 売却時の記録 |
| 貸手リース案件名 | 貸手リース案件の名称 | `貸手リース案件名` | 貸手リース時の記録 |

### BigQueryで新たに準備が必要なデータ

| データ項目 | 現状 | 対応方針 |
|-----------|------|---------|
| **耐用年数** | 不明 | パーツマスタに追加、または固定値で設定 |
| **減損損失日** | 不明 | 在庫テーブルに追加、または減損ロジックを別途実装 |
| **サプライヤー名** | 存在する可能性あり | `lake.supplier` テーブルを確認 |
| **貸手リース関連日付** | 不明 | ビジネス要件を確認して追加の要否を判断 |
| **初回出荷日** | 計算ロジックあり | `lake.stock` または履歴テーブルから取得 |

---

## SQL移植の方針

### 全体構成

```sql
WITH
-- ステップ1: 基礎データの準備
stock_with_supplier AS (
  SELECT s.*, sup.name AS supplier_name, p.useful_life_years
  FROM lake.stock s
  LEFT JOIN lake.supplier sup ON s.supplier_id = sup.id
  LEFT JOIN lake.part p ON s.part_id = p.id
),

-- ステップ2: 資産分類・会計ステータスの決定
stock_with_classification AS (
  SELECT *,
    CASE
      WHEN sample = TRUE THEN 'サンプル品'
      WHEN supplier_name IS NULL THEN '賃貸用固定資産'
      WHEN supplier_name LIKE '%リース・レベシェア%' THEN 'リース資産(借手リース)'
      WHEN supplier_name LIKE '%レベシェア%' THEN 'レベシェア品'
      WHEN supplier_name LIKE '%法人小物管理用%' THEN '小物等'
      ELSE '賃貸用固定資産'
    END AS asset_classification
  FROM stock_with_supplier
),

-- ステップ3: 初回出荷日・リース再取得日の計算
stock_with_dates AS (
  SELECT *,
    CASE
      WHEN supplier_name LIKE '株式会社カンム 契約No.2022000%' THEN
        PARSE_DATE('%Y-%m-01', CONCAT('2022-', REGEXP_EXTRACT(supplier_name, r'2022000(\d)'), '-01'))
      -- 他のパターンも同様に
      ELSE first_shipped_at  -- 既存データを使用
    END AS calculated_first_shipped_at,
    -- リース再取得日も同様のロジック
  FROM stock_with_classification
),

-- ステップ4: 償却α/β/γの計算
stock_with_shokyaku AS (
  SELECT *,
    -- calculate_shokyaku_alpha のロジック
    CASE
      WHEN impairment_date IS NOT NULL AND impairment_date < @start_date THEN
        DATE_DIFF(impairment_date, calculated_first_shipped_at, MONTH) + 1
      -- 他の条件分岐
      ELSE DATE_DIFF(@start_date, calculated_first_shipped_at, MONTH)
    END AS shokyaku_alpha,
    -- shokyaku_beta, shokyaku_gamma も同様
  FROM stock_with_dates
),

-- ステップ5: 取得原価の計算
stock_with_acquisition_cost AS (
  SELECT *,
    -- 期首取得原価
    CASE
      WHEN (supplier_name LIKE '%レベシェア%' AND supplier_name NOT LIKE '%リース・レベシェア%')
           OR supplier_name LIKE '%法人小物管理用%' OR sample = TRUE THEN 0
      WHEN inspected_at IS NULL OR inspected_at >= @start_date THEN 0
      -- 他の条件
      ELSE cost
    END AS acquisition_cost_kishu,
    -- 増加取得原価、減少取得原価、期末取得原価も同様
  FROM stock_with_shokyaku
),

-- ステップ6: 償却月数の計算
stock_with_amortization_months AS (
  SELECT *,
    LEAST(shokyaku_alpha, useful_life_years * 12) AS amortization_months_kishu,
    -- 他の償却月数も同様
  FROM stock_with_acquisition_cost
),

-- ステップ7: 減価償却累計額の計算
stock_with_depreciation AS (
  SELECT *,
    cost / (useful_life_years * 12) AS monthly_depreciation,
    CASE
      WHEN useful_life_years * 12 <= amortization_months_kishu THEN acquisition_cost_kishu
      WHEN acquisition_cost_kishu < amortization_months_kishu THEN acquisition_cost_kishu
      ELSE (cost / (useful_life_years * 12)) * amortization_months_kishu
    END AS accumulated_depreciation_kishu,
    -- 他の減価償却累計額も同様
  FROM stock_with_amortization_months
),

-- ステップ8: 減損損失累計額の計算
stock_with_impairment AS (
  SELECT *,
    CASE
      WHEN impairment_date IS NOT NULL AND impairment_date < @start_date THEN
        GREATEST(0, acquisition_cost_kishu - accumulated_depreciation_kishu)
      ELSE 0
    END AS impairment_loss_accumulated_kishu,
    -- 他の減損損失累計額も同様
  FROM stock_with_depreciation
),

-- ステップ9: 簿価の計算
stock_with_book_value AS (
  SELECT *,
    acquisition_cost_kishu - accumulated_depreciation_kishu - impairment_loss_accumulated_kishu AS opening_book_value,
    acquisition_cost_increase - accumulated_depreciation_increase - impairment_loss_accumulated_increase AS increase_book_value,
    acquisition_cost_decrease - accumulated_depreciation_decrease - impairment_loss_accumulated_decrease AS decrease_book_value,
    CASE
      WHEN accounting_status IN ('賃貸用固定資産', 'リース資産(借手リース)')
      THEN GREATEST(0, acquisition_cost_kimatsu - accumulated_depreciation_kimatsu - impairment_loss_accumulated_kimatsu)
      ELSE 0
    END AS closing_book_value
  FROM stock_with_impairment
)

SELECT * FROM stock_with_book_value
```

### 移植時の注意点

1. **日付計算**: `pd.to_datetime()` → `PARSE_DATE()`, `DATE_DIFF()`
2. **NULL処理**: `pd.isna()` / `pd.notna()` → `IS NULL` / `IS NOT NULL`
3. **文字列マッチ**: `re.search()` → `REGEXP_CONTAINS()`, `REGEXP_EXTRACT()`
4. **月の計算**: `calculate_months_diff()` → `DATE_DIFF(date1, date2, MONTH)`
5. **最小・最大**: `min()` / `max()` → `LEAST()` / `GREATEST()`
6. **条件分岐**: `if-elif-else` → `CASE WHEN ... THEN ... ELSE ... END`

### パフォーマンス対策

- **CTE分割**: 各ステップをCTEに分けて可読性・デバッグ性を確保
- **早期フィルタ**: 簿価計算対象外（レベシェア品、サンプル品等）を早期に除外
- **インデックス活用**: `stock_id`, `part_id`, `supplier_id` でJOIN最適化
- **パラメータ使用**: `@start_date`, `@end_date` で期間指定

### テスト方針

1. **具体的なstock_idで検証**: Pythonで計算した結果とSQLの結果を比較
2. **特殊ケースのテスト**:
   - リース再取得があるケース
   - 減損損失があるケース
   - 除売却・貸手リース転換があるケース
   - 耐用年数到達済みのケース
3. **期待値テーブル作成**: 各stock_idについて期待値を事前に用意

---

## まとめ

### 計算の本質

```
簿価 = 取得原価 - 減価償却累計額 - 減損損失累計額
```

### 複雑な部分

1. **時系列の追跡**: 期首・期中増加・期中減少・期末の4時点を追跡
2. **特殊イベント処理**: リース再取得、減損損失、除売却、貸手リース転換
3. **条件分岐の多さ**: 資産分類、会計ステータス、日付の前後関係など
4. **サプライヤー名パターンマッチ**: 特定のサプライヤー名から日付や分類を推測

### SQL移植の優先度

| 優先度 | 項目 | 理由 |
|-------|------|------|
| 高 | 取得原価の計算 | 簿価の基礎となるデータ |
| 高 | 償却α/β/γの計算 | 減価償却の基準値 |
| 高 | 減価償却累計額の計算 | 簿価計算に必須 |
| 中 | 減損損失累計額の計算 | ビジネス要件による |
| 中 | リース関連の処理 | 該当データがあれば必要 |
| 低 | サプライヤー名パターンマッチ | データ整備で代替可能 |

### 次のステップ

1. BigQueryでのデータ存在確認（特に耐用年数、減損損失日）
2. 不足データの取得方法検討
3. テストケース用のstock_id選定
4. SQL実装（段階的にCTEを追加）
5. Pythonとの結果比較検証
