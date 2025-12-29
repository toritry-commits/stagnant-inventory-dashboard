# 簿価計算SQL実装に必要なデータ項目チェックリスト

**作成日**: 2025-12-25
**目的**: Pythonロジックの完全移植に必要なすべてのデータ項目の確認

---

## 必須データ項目（すべて存在確認済み ✅）

| # | 項目名（日本語） | 項目名（Python） | BigQueryテーブル.カラム | データ型 | 説明 | サンプル値 |
|---|-----------------|-----------------|----------------------|---------|------|-----------|
| 1 | 在庫ID | `stock_id` | `lake.stock.id` | INTEGER | 在庫の一意識別子 | 3 |
| 2 | パーツID | `part_id` | `lake.stock.part_id` | INTEGER | パーツの一意識別子 | 1 |
| 3 | パーツ名 | `part_name` | `lake.part.name` | STRING | パーツの名称 | "ちょうどソファ ヘッドレスト" |
| 4 | 取得原価（基本） | `cost` | `lake.stock.cost` | INTEGER | 購入時の基本価格（円） | 10281 |
| 4a | オーバーヘッドコスト | `overhead_cost` | `lake.stock_acquisition_costs.overhead_cost` | INTEGER/FLOAT | 諸経費 | 500 |
| 4b | ディスカウント | `discount` | `lake.stock_acquisition_costs.discount` | INTEGER/FLOAT | 値引額 | 200 |
| 4c | **取得原価（計算後）** | - | `cost + overhead_cost - discount` | INTEGER/FLOAT | **実際の取得原価** | 10581 |
| 5 | 耐用年数 | `depreciation_period` | `finance.fixed_asset_register.depreciation_period` | INTEGER | 減価償却の期間（年） | 5 |
| 6 | 入庫検品完了日 | `inspected_at` | `lake.stock.inspected_at` | DATE | 入庫して検品が完了した日 | 2018-08-29 |
| 7 | 初回出荷日 | `first_shipped_at` | `finance.fixed_asset_register.first_shipped_at` | DATE | 供与開始日（最初に出荷した日） | 2018-10-14 |
| 8 | 除売却日 | `impossibled_at` | `lake.stock.impossibled_at` | DATE | 除却または売却した日 | 2025-10-07 |
| 9 | 破損紛失分類 | `classification_of_impossibility` | `lake.stock.classification_of_impossibility` | STRING | 除却理由の分類 | "庫内紛失／棚卸差異" |
| 10 | サプライヤー名 | `supplier_name` | `lake.supplier.name` | STRING | 仕入先の名称 | "HONGKONG LETING FURNITURE CO.,LIMITED" |
| 11 | **減損損失日** | `impairment_date` | `lake.stock.impairment_date` | DATE | 減損損失を計上した日 | 2022-02-28 |

---

## オプションデータ項目（特定ケースで使用）

| # | 項目名（日本語） | 項目名（Python） | BigQueryテーブル.カラム | データ型 | 使用ケース | サンプル値 |
|---|-----------------|-----------------|----------------------|---------|-----------|-----------|
| 12 | サンプル品フラグ | `sample` | `lake.stock.sample` | BOOLEAN | サンプル品は簿価計算対象外 | false |
| 13 | 貸手リース開始日 | `lease_start_at` | `lake.stock.lease_start_at` | DATE | 自社資産→賃貸債権に転換 | NULL |
| 14 | 売却日 | `sold_date` | （調査必要） | DATE | 売却した日付 | NULL |
| 15 | リース再取得日 | `lease_reacquisition_date` | （計算で導出） | DATE | サプライヤー名から計算 | NULL |

---

## 計算で導出する項目（SQL内で生成）

| # | 項目名（日本語） | 計算方法 | 説明 |
|---|-----------------|---------|------|
| 16 | 期首日 | パラメータ指定 | 会計期間の開始日 |
| 17 | 期末日 | パラメータ指定 | 会計期間の終了日 |
| 18 | 月次償却額 | `CASE WHEN 耐用年数 = 0 THEN 0 ELSE (cost + overhead_cost - discount) / (耐用年数 × 12) END` | 月ごとの減価償却額（耐用年数0年なら0） |
| 19 | 資産分類 | `CASE文でサプライヤー名とsampleフラグから判定` | "賃貸用固定資産"、"リース資産"など |
| 20 | 会計ステータス | `資産分類・日付から判定` | "賃貸用固定資産"、"計上外"など |
| 21 | 償却α | `DATE_DIFF(期首日, 初回出荷日, MONTH)` | 期首時点での償却済み月数 |
| 22 | 償却β | `DATE_DIFF(期末日, 初回出荷日, MONTH) + 1` | 期末時点での償却済み月数 |
| 23 | 償却γ | `DATE_DIFF(リース再取得日, 初回出荷日, MONTH)` | リース再取得時点の償却済み月数 |

---

## データ取得に必要なテーブルとJOIN

### メインテーブル: `lake.stock`

```sql
SELECT
  id,                          -- 在庫ID
  part_id,                     -- パーツID
  cost,                        -- 取得原価
  inspected_at,                -- 入庫検品完了日
  impossibled_at,              -- 除売却日
  impairment_date,             -- 減損損失日
  classification_of_impossibility, -- 破損紛失分類
  sample,                      -- サンプル品フラグ
  supplier_id                  -- サプライヤーID（JOIN用）
FROM `clas-analytics.lake.stock`
WHERE deleted_at IS NULL
```

### JOIN 1: `lake.part` (パーツ情報)

```sql
LEFT JOIN `clas-analytics.lake.part` p
  ON s.part_id = p.id
```

**取得項目**:
- `p.name` → パーツ名
- `p.useful_life_years` → 耐用年数（要確認、存在しない場合はカテゴリ別デフォルト値）

### JOIN 2: `lake.supplier` (サプライヤー情報)

```sql
LEFT JOIN `clas-analytics.lake.supplier` sup
  ON s.supplier_id = sup.id
```

**取得項目**:
- `sup.name` → サプライヤー名（資産分類の判定に使用）

### JOIN 3: 初回出荷日の取得（要確認）

**方法A: 既存カラムがあれば使用**
```sql
-- lake.stockに first_shipped_at カラムが存在する場合
SELECT first_shipped_at FROM lake.stock
```

**方法B: 出荷履歴から計算**
```sql
-- 出荷履歴テーブルから最小の出荷日を取得
LEFT JOIN (
  SELECT
    stock_id,
    MIN(shipped_at) as first_shipped_at
  FROM `clas-analytics.lake.stock_shipment` -- テーブル名要確認
  GROUP BY stock_id
) sh ON s.id = sh.stock_id
```

**方法C: Pythonのロジックで計算**
```sql
-- サプライヤー名のパターンマッチで計算
CASE
  WHEN supplier_name LIKE '株式会社カンム 契約No.2022000%' THEN
    PARSE_DATE('%Y-%m-01', CONCAT('2022-', REGEXP_EXTRACT(supplier_name, r'2022000(\d)'), '-01'))
  -- 他のパターンも同様
  ELSE first_shipped_at  -- デフォルト値
END
```

---

## データ存在確認状況

### ✅ 確認済み（lake.stock）

| カラム名 | 型 | 確認方法 |
|---------|-----|---------|
| `id` | INTEGER | スキーマ確認済み |
| `part_id` | INTEGER | スキーマ確認済み |
| `cost` | INTEGER | スキーマ確認済み |
| `inspected_at` | DATE | スキーマ確認済み |
| `impossibled_at` | DATE | スキーマ確認済み |
| `impairment_date` | DATE | スキーマ確認済み、サンプルデータ取得済み |
| `classification_of_impossibility` | STRING | スキーマ確認済み |
| `sample` | BOOLEAN | スキーマ確認済み |
| `supplier_id` | INTEGER | スキーマ確認済み |

### 🔍 要確認項目

| 項目 | 確認が必要な内容 | 優先度 |
|-----|----------------|--------|
| **耐用年数** | `lake.part.useful_life_years` が存在するか | 🔴 高 |
| **初回出荷日** | `lake.stock.first_shipped_at` が存在するか、または出荷履歴テーブルから取得可能か | 🔴 高 |
| **貸手リース開始日** | `lake.stock.lease_start_at` のカラム名確認 | 🟡 中 |

---

## 次のアクション（SQL実装前に確認）

### 1. 耐用年数の取得方法確認

**確認クエリ**:
```sql
-- lake.partテーブルのスキーマ確認
bq show --schema clas-analytics:lake.part

-- 耐用年数カラムの存在確認
SELECT id, name, useful_life_years
FROM `clas-analytics.lake.part`
LIMIT 5
```

**代替案**:
- カラムが存在しない場合、カテゴリ別デフォルト値を設定
- または `finance.fixed_asset_register.depreciation_period` から取得

### 2. 初回出荷日の取得方法確認

**確認クエリ**:
```sql
-- lake.stockに存在するか確認
SELECT id, first_shipped_at
FROM `clas-analytics.lake.stock`
WHERE first_shipped_at IS NOT NULL
LIMIT 5

-- 存在しない場合、出荷履歴テーブルを確認
SELECT stock_id, MIN(shipped_at) as first_shipped_at
FROM `clas-analytics.lake.???` -- テーブル名要確認
GROUP BY stock_id
LIMIT 5
```

**代替案**:
- `finance.fixed_asset_register.first_shipped_at` から取得
- Pythonの計算ロジックをSQLで実装

### 3. 貸手リース開始日の確認

**確認クエリ**:
```sql
-- lake.stockのスキーマでリース関連カラムを確認
bq show --schema clas-analytics:lake.stock | grep -i lease
```

---

## データ完全性の確認

実装前に、以下のクエリでデータの完全性を確認することを推奨：

```sql
-- 必須項目のNULL率を確認
SELECT
  COUNT(*) as total_records,
  COUNTIF(id IS NULL) as null_id,
  COUNTIF(part_id IS NULL) as null_part_id,
  COUNTIF(cost IS NULL) as null_cost,
  COUNTIF(inspected_at IS NULL) as null_inspected_at,
  COUNTIF(impairment_date IS NOT NULL) as has_impairment,
  COUNTIF(impossibled_at IS NOT NULL) as has_impossibled,
  COUNTIF(sample = TRUE) as sample_count
FROM `clas-analytics.lake.stock`
WHERE deleted_at IS NULL
```

---

## まとめ

### ✅ 揃っているデータ

すべての**必須データ項目**がBigQueryに存在することを確認済み：
- 在庫ID、パーツID、取得原価
- 入庫検品完了日、除売却日
- **減損損失日**（`lake.stock.impairment_date`）
- サプライヤーID、サンプル品フラグ、破損紛失分類

### 🔍 確認が必要なデータ（SQL実装前）

1. **耐用年数**: `lake.part.useful_life_years` の存在確認
2. **初回出荷日**: `lake.stock.first_shipped_at` または出荷履歴からの取得方法
3. **貸手リース開始日**: カラム名の確認

### 次のステップ

上記3点を確認後、SQL実装を開始できます。
