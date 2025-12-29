# 簿価計算SQL実装 最終データ項目チェックリスト

**作成日**: 2025-12-25
**更新日**: 2025-12-25
**目的**: Pythonロジック完全移植に必要なすべてのデータ項目の最終確認

---

## ✅ 存在確認済み - 必須データ項目

| # | 項目名 | Pythonカラム名 | BigQueryソース | データ型 | 説明 | サンプル値 |
|---|--------|---------------|---------------|---------|------|-----------|
| 1 | 在庫ID | `在庫id` | `lake.stock.id` | INTEGER | 在庫の一意識別子 | 3 |
| 2 | パーツID | `パーツid` | `lake.stock.part_id` | INTEGER | パーツの一意識別子 | 1 |
| 3 | パーツ名 | `パーツ名` | `lake.part.name` | STRING | パーツの名称 | "ちょうどソファ ヘッドレスト" |
| 4 | **取得原価（基本）** | `取得原価` | `lake.stock.cost` | INTEGER | 購入時の基本価格 | 10281 |
| 5 | **オーバーヘッドコスト** | - | `lake.stock_acquisition_costs.overhead_cost` | FLOAT | 諸経費（加算） | 500 |
| 6 | **ディスカウント** | - | `lake.stock_acquisition_costs.discount` | FLOAT | 値引額（減算） | 200 |
| 7 | **実際の取得原価** | `取得原価` | **計算式**: `cost + overhead_cost - discount` | FLOAT | **最終的な取得原価** | 10581 |
| 8 | **耐用年数** | `耐用年数` | `finance.fixed_asset_register.depreciation_period` | INTEGER | 減価償却の期間（年） | 5 |
| 9 | **入庫検品完了日** | `入庫検品完了日` | `lake.stock.inspected_at` | DATE | 入庫して検品が完了した日 | 2018-08-29 |
| 10 | **初回出荷日** | `供与開始日(初回出荷日)` | `finance.fixed_asset_register.first_shipped_at` | DATE | 供与開始日（最初に出荷した日） | 2018-10-14 |
| 11 | 除売却日 | `除売却日` | `lake.stock.impossibled_at` | DATE | 除却または売却した日 | 2025-10-07 |
| 12 | 破損紛失分類 | `破損紛失分類` | `lake.stock.classification_of_impossibility` | STRING | 除却理由の分類 | "庫内紛失／棚卸差異" |
| 13 | サプライヤー名 | `サプライヤー名` | `lake.supplier.name` | STRING | 仕入先の名称 | "HONGKONG LETING..." |
| 14 | **減損損失日** | `減損損失日` | `lake.stock.impairment_date` | DATE | 減損損失を計上した日 | 2022-02-28 |
| 15 | サンプル品フラグ | `sample` | `lake.stock.sample` | BOOLEAN | サンプル品は簿価計算対象外 | false |
| 16 | **貸手リース開始日** | `貸手リース開始日` | `finance.fixed_asset_register.lease_start_at` | DATE | 自社資産→賃貸債権に転換した日 | NULL |

---

## ✅ サプライヤー名から計算する項目（Looker Studio実装を参考）

### 契約識別コード

```sql
REGEXP_EXTRACT(supplier_name, '_契約開始(\\d{4})$') AS contract_code
```

### 初回出荷日（サプライヤー名から計算）

```sql
CASE
  -- 株式会社カンム パターン
  WHEN REGEXP_CONTAINS(supplier_name, r"^株式会社カンム 契約No\.2022000(\d)$")
  THEN PARSE_DATE("%Y-%m-%d",
    CONCAT("2022-", REGEXP_EXTRACT(supplier_name, r"契約No\.2022000(\d)"), "-1"))

  -- 三井住友トラスト・パナソニックファイナンス パターン
  WHEN STARTS_WITH(supplier_name, "三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始")
       AND contract_code IS NOT NULL
       AND LENGTH(contract_code) = 4
  THEN LAST_DAY(
    DATE(
      CAST(CONCAT("20", SUBSTR(contract_code, 1, 2)) AS INT64),
      CAST(SUBSTR(contract_code, 3, 2) AS INT64),
      1
    )
  )

  -- デフォルト: fixed_asset_registerから取得した値を使用
  ELSE first_shipped_at
END AS calculated_first_shipped_at
```

### リース再取得日

```sql
CASE
  -- 株式会社カンム パターン（2024年の該当月の1日）
  WHEN REGEXP_CONTAINS(supplier_name, r"^株式会社カンム 契約No\.2022000(\d)$")
  THEN PARSE_DATE("%Y-%m-%d",
    CONCAT("2024-", REGEXP_EXTRACT(supplier_name, r"契約No\.2022000(\d)"), "-1"))

  -- 三井住友トラスト・パナソニックファイナンス パターン（契約開始から30ヶ月後の月末）
  WHEN STARTS_WITH(supplier_name, "三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始")
       AND contract_code IS NOT NULL
       AND LENGTH(contract_code) = 4
  THEN DATE_SUB(
    DATE_TRUNC(
      DATE_ADD(
        DATE_ADD(
          DATE(
            CAST(CONCAT("20", SUBSTR(contract_code, 1, 2)) AS INT64),
            CAST(SUBSTR(contract_code, 3, 2) AS INT64),
            1
          ),
          INTERVAL 30 MONTH
        ),
        INTERVAL 1 MONTH
      ),
      MONTH
    ),
    INTERVAL 1 DAY
  )

  ELSE NULL
END AS lease_reacquisition_date
```

**計算ロジック解説**:

1. **株式会社カンム**:
   - 契約No.末尾の数字（5, 6など）を月として使用
   - 2024年の該当月の1日がリース再取得日
   - 例: `契約No.20220005` → `2024-5-1`

2. **三井住友トラスト・パナソニックファイナンス**:
   - 契約識別コード（例: `2102`）から年月を抽出
   - `20` + 上2桁 = 年（例: `2021`）
   - 下2桁 = 月（例: `02`）
   - その月の1日から30ヶ月後の前月末がリース再取得日
   - 例: `2102` → `2021-02-01` + 30ヶ月 = `2023-08-31`

---

## 計算で導出する項目（SQL内で生成）

| # | 項目名 | 計算式 | 説明 |
|---|--------|--------|------|
| 18 | **月次償却額** | `CASE WHEN 耐用年数 = 0 THEN 0 ELSE (cost + overhead_cost - discount) / (耐用年数 × 12) END` | 月ごとの減価償却額（耐用年数0年なら0） |
| 19 | 期首日 | パラメータ指定 | 会計期間の開始日（例: 2025-03-01） |
| 20 | 期末日 | パラメータ指定 | 会計期間の終了日（例: 2026-02-28） |
| 21 | 資産分類 | CASE文 | サプライヤー名とsampleフラグから判定 |
| 22 | 会計ステータス | CASE文 | 資産分類・日付から判定 |
| 23 | 償却α | DATE_DIFF + 複雑な条件分岐 | 期首時点での償却済み月数 |
| 24 | 償却β | DATE_DIFF + 複雑な条件分岐 | 期末時点での償却済み月数 |
| 25 | 償却γ | DATE_DIFF + 複雑な条件分岐 | リース再取得時点の償却済み月数 |
| 26 | 期首/増加/減少/期末 取得原価 | 日付範囲とステータスで判定 | 4時点の取得原価 |
| 27 | 期首/増加/減少/期末 償却月数 | 償却α/β/γから計算 | 4時点の償却月数 |
| 28 | 期首/増加/減少/期末 減価償却累計額 | 月次償却額 × 償却月数 | 4時点の減価償却累計額 |
| 29 | 期首/増加/減少/期末 減損損失累計額 | 減損日と簿価から計算 | 4時点の減損損失累計額 |
| 30 | **期首/増加/減少/期末 簿価** | 取得原価 - 減価償却累計額 - 減損損失累計額 | **最終的な帳簿価額** |

---

## 必要なテーブルとJOIN構造

### メインテーブル: `lake.stock`

```sql
SELECT
  s.id,
  s.part_id,
  s.cost,
  s.inspected_at,
  s.impossibled_at,
  s.impairment_date,
  s.classification_of_impossibility,
  s.sample,
  s.supplier_id
FROM `clas-analytics.lake.stock` s
WHERE s.deleted_at IS NULL
```

### JOIN 1: `lake.stock_acquisition_costs` (取得原価の補正)

```sql
LEFT JOIN `clas-analytics.lake.stock_acquisition_costs` sac
  ON s.id = sac.stock_id
```

**取得項目**:
- `sac.overhead_cost` - オーバーヘッドコスト（FLOAT）
- `sac.discount` - ディスカウント（FLOAT）

**計算**:
```sql
COALESCE(s.cost, 0) + COALESCE(sac.overhead_cost, 0) - COALESCE(sac.discount, 0) AS actual_cost
```

### JOIN 2: `lake.part` (パーツ情報)

```sql
LEFT JOIN `clas-analytics.lake.part` p
  ON s.part_id = p.id
```

**取得項目**:
- `p.name` - パーツ名

### JOIN 3: `lake.supplier` (サプライヤー情報)

```sql
LEFT JOIN `clas-analytics.lake.supplier` sup
  ON s.supplier_id = sup.id
```

**取得項目**:
- `sup.name` - サプライヤー名（資産分類の判定に使用）

### JOIN 4: `finance.fixed_asset_register` (耐用年数と初回出荷日)

```sql
LEFT JOIN (
  SELECT stock_id, depreciation_period, first_shipped_at, lease_start_at
  FROM `clas-analytics.finance.fixed_asset_register`
  WHERE term = (SELECT MAX(term) FROM `clas-analytics.finance.fixed_asset_register`)
) far
  ON s.id = far.stock_id
```

**取得項目**:
- `far.depreciation_period` - 耐用年数（INTEGER）
- `far.first_shipped_at` - 初回出荷日（DATE）
- `far.lease_start_at` - 貸手リース開始日（DATE）

**注意点**:
- `fixed_asset_register`は期（term）ごとにレコードが存在するため、最新期のみを取得

---

## データ品質の確認クエリ

### 取得原価の計算確認

```sql
SELECT
  s.id,
  s.cost as base_cost,
  sac.overhead_cost,
  sac.discount,
  s.cost + COALESCE(sac.overhead_cost, 0) - COALESCE(sac.discount, 0) as actual_cost
FROM `clas-analytics.lake.stock` s
LEFT JOIN `clas-analytics.lake.stock_acquisition_costs` sac
  ON s.id = sac.stock_id
WHERE s.id IN (3, 1211, 2669)
```

### 必須項目のNULL率確認

```sql
SELECT
  COUNT(*) as total,
  COUNTIF(s.cost IS NULL) as null_cost,
  COUNTIF(s.inspected_at IS NULL) as null_inspected,
  COUNTIF(s.impairment_date IS NOT NULL) as has_impairment,
  COUNTIF(sac.overhead_cost IS NOT NULL) as has_overhead,
  COUNTIF(sac.discount IS NOT NULL) as has_discount,
  COUNTIF(far.depreciation_period IS NULL) as null_depreciation_period,
  COUNTIF(far.first_shipped_at IS NULL) as null_first_shipped
FROM `clas-analytics.lake.stock` s
LEFT JOIN `clas-analytics.lake.stock_acquisition_costs` sac ON s.id = sac.stock_id
LEFT JOIN (
  SELECT stock_id, depreciation_period, first_shipped_at
  FROM `clas-analytics.finance.fixed_asset_register`
  WHERE term = (SELECT MAX(term) FROM `clas-analytics.finance.fixed_asset_register`)
) far ON s.id = far.stock_id
WHERE s.deleted_at IS NULL
LIMIT 1
```

---

## まとめ

### ✅ すべて揃っているデータ（18項目）

| # | 項目 | 取得元 |
|---|------|--------|
| 1 | 在庫ID | `lake.stock.id` |
| 2 | パーツID | `lake.stock.part_id` |
| 3 | パーツ名 | `lake.part.name` |
| 4 | **取得原価** | `cost + overhead_cost - discount` |
| 5 | オーバーヘッドコスト | `lake.stock_acquisition_costs.overhead_cost` |
| 6 | ディスカウント | `lake.stock_acquisition_costs.discount` |
| 7 | **耐用年数** | `finance.fixed_asset_register.depreciation_period` |
| 8 | 入庫検品完了日 | `lake.stock.inspected_at` |
| 9 | **初回出荷日** | サプライヤー名から計算 + `finance.fixed_asset_register.first_shipped_at` |
| 10 | 除売却日 | `lake.stock.impossibled_at` |
| 11 | 破損紛失分類 | `lake.stock.classification_of_impossibility` |
| 12 | サプライヤー名 | `lake.supplier.name` |
| 13 | **減損損失日** | `lake.stock.impairment_date` |
| 14 | サンプル品フラグ | `lake.stock.sample` |
| 15 | **貸手リース開始日** | `finance.fixed_asset_register.lease_start_at` |
| 16 | **リース再取得日** | **サプライヤー名から計算**（REGEXP_EXTRACT使用） |
| 17 | 契約識別コード | **サプライヤー名から抽出**（中間値） |
| 18 | 期首日・期末日 | パラメータ指定 |

### ✅ すべてのデータ項目が確定

**リース再取得日**と**初回出荷日**のサプライヤー名からの計算ロジックも確定しました。

### 📝 重要な計算式

**実際の取得原価**:
```sql
COALESCE(s.cost, 0)
  + COALESCE(sac.overhead_cost, 0)
  - COALESCE(sac.discount, 0) AS actual_cost
```

**月次償却額**:
```sql
CASE
  WHEN COALESCE(far.depreciation_period, 0) = 0 THEN 0
  ELSE actual_cost / (far.depreciation_period * 12)
END AS monthly_depreciation
```

---

## 次のステップ

1. ✅ すべての必須データ項目の存在確認完了
2. ⏳ **リース再取得日についてユーザーからの補足情報を待つ**
3. ⏸️ SQL実装は保留中（ユーザー指示待ち）

リース再取得日の補足情報をいただければ、SQL実装の準備が完了します。
