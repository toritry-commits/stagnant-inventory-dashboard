# 依存関係リファクタリング設計書

**作成日**: 2026-01-11
**ステータス**: 完了

---

## 概要

滞留在庫ダッシュボードと月次簿価計算クエリにおいて、不要な外部テーブル依存を解消しました。

### 対象ファイル

| ファイル | 変更内容 |
|----------|----------|
| `stagnant_inventory_dashboard_v3.sql` | finance依存を削除 |
| `monthly_stock_valuation_v2.sql` | finance/mart依存を完全に削除 |

---

## 背景と目的

### 課題

元のクエリは以下の外部テーブルに依存していました:

| テーブル | 用途 |
|----------|------|
| `finance.fixed_asset_register` | 耐用年数、初回出荷日、リース開始日 |
| `mart.stock_info` | 売却案件名、リース案件名 |
| `mart.monthly_stock_valuation` | 簿価データ |

これらの依存により:
- テーブル更新順序に制約が発生
- スケジューリングが複雑化
- 障害時の影響範囲が拡大

### 目的

- `lake.*` テーブルのみへの依存に変更し、データパイプラインをシンプル化
- 外部テーブルの更新を待たずにクエリ実行可能にする

---

## 変更内容

### 1. monthly_stock_valuation_v2.sql (Lake Only版)

**削除した依存:**
- `finance.fixed_asset_register` → CTE で計算
- `mart.stock_info` → CTE で計算

**追加したCTE:**

| CTE名 | 元の依存 | 実装内容 |
|-------|----------|----------|
| `lent_info` | fixed_asset_register | lake.lent + lake.lent_detail から初回出荷日を計算 |
| `to_b_info` | fixed_asset_register | lake.lent_to_b + lake.lent_to_b_detail からリース開始日を計算 |
| `sold_proposition` | mart.stock_info | lake テーブルから売却案件名を取得 |
| `lease_proposition` | mart.stock_info | lake テーブルからリース案件名を取得 |

**検証結果:**
- v1 と v2 の出力結果が完全一致することを確認
  - total_stocks: 171,044
  - active_stocks: 68,923
  - total_acquisition_cost: 1,656,001,191
  - reveshare_count: 7,226
  - lease_count: 12,841

### 2. stagnant_inventory_dashboard_v3.sql

**削除した依存:**
- `finance.fixed_asset_register` → `lake.stock.arrival_at` を直接使用

**変更箇所:**

```sql
-- 変更前 (v2/v4)
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    COALESCE(MAX(fa.arrival_at), ANY_VALUE(s.arrival_at)) AS arrival_at
  FROM `lake.stock` s
  LEFT JOIN `finance.fixed_asset_register` fa ON s.id = fa.stock_id
  WHERE s.deleted_at IS NULL
  GROUP BY s.id, s.part_id
),

-- 変更後 (v3)
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    s.arrival_at
  FROM `lake.stock` s
  WHERE s.deleted_at IS NULL
),
```

**継続する依存:**
- `mart.monthly_stock_valuation` → 簿価データ取得に必要
  - スケジュール更新の調整で対応

---

## フォルダ構成

```
sql/
├── stagnant_inventory_dashboard_v3.sql  # 最新版 (finance依存なし)
├── monthly_stock_valuation_v2.sql       # Lake Only版
├── select_stock_valuation_summary.sql
├── archive/                              # アーカイブ
│   ├── stagnant_inventory_dashboard_v2.sql
│   ├── stagnant_inventory_dashboard_v3.sql (旧版)
│   ├── stagnant_inventory_dashboard_v4.sql
│   ├── monthly_stock_valuation.sql
│   ├── select_fixed_asset_register_monthly.sql
│   └── term1_future_orders.sql
└── reference/                            # 参照用 (元クエリの依存解析用)
    ├── fixed_asset_register.sql
    ├── stock_info.sql
    └── stock_report.sql
```

---

## 依存関係サマリ

### 変更前

```
stagnant_inventory_dashboard
  └── finance.fixed_asset_register
  └── mart.monthly_stock_valuation
        └── finance.fixed_asset_register
        └── mart.stock_info
              └── mart.stock_report
                    └── warehouse.product_category
```

### 変更後

```
stagnant_inventory_dashboard_v3
  └── mart.monthly_stock_valuation (スケジュール調整で対応)
  └── lake.* (その他すべて)

monthly_stock_valuation_v2
  └── lake.* のみ
```

---

## 参照テーブル詳細 (stock_info.sql)

`sql/reference/stock_info.sql` が参照しているテーブルの一覧です。

### lake層 (19テーブル)

| テーブル名 | 用途 |
|-----------|------|
| `lake.stock` | 在庫マスタ (メインテーブル) |
| `lake.part` | 部品/商品マスタ |
| `lake.supplier` | 仕入先マスタ |
| `lake.location` | ロケーション |
| `lake.warehouse` | 倉庫マスタ |
| `lake.lent` | C向け貸出 |
| `lake.lent_detail` | C向け貸出明細 |
| `lake.lent_to_b` | B向け貸出 |
| `lake.lent_to_b_detail` | B向け貸出明細 |
| `lake.proposition` | 提案/案件 |
| `lake.contract` | 契約 |
| `lake.contract_destination` | 契約送付先 |
| `lake.client` | クライアント |
| `lake.purchasing` | 仕入 |
| `lake.purchasing_detail` | 仕入明細 |
| `lake.purchasing_detail_stock` | 仕入明細-在庫紐付け |
| `lake.external_sale_stock` | 外部販売在庫 |
| `lake.external_sale_product` | 外部販売商品 |
| `lake.stock_acquisition_costs` | 在庫取得原価 |

### mart層 (1テーブル)

| テーブル名 | 用途 |
|-----------|------|
| `mart.stock_report` | 在庫レポート (カテゴリ情報取得用) |

### 依存関係図

```
stock_info.sql
    |
    +-- lake.stock (中心)
    |       |-- lake.part
    |       |-- lake.supplier
    |       |-- lake.location --> lake.warehouse
    |       +-- lake.stock_acquisition_costs
    |
    +-- 貸出系 (C向け)
    |       |-- lake.lent
    |       +-- lake.lent_detail
    |
    +-- 貸出系 (B向け)
    |       |-- lake.lent_to_b
    |       |-- lake.lent_to_b_detail
    |       |-- lake.contract_destination
    |       |-- lake.contract
    |       |-- lake.proposition
    |       +-- lake.client
    |
    +-- 仕入系
    |       |-- lake.purchasing
    |       |-- lake.purchasing_detail
    |       +-- lake.purchasing_detail_stock
    |
    +-- 外部販売系
    |       |-- lake.external_sale_stock
    |       +-- lake.external_sale_product
    |
    +-- mart.stock_report (カテゴリ情報)
```

---

## 今後の対応

1. **mart.monthly_stock_valuation のスケジュール調整**
   - stagnant_inventory_dashboard より先に更新されるよう設定

2. **monthly_stock_valuation_v2 の本番適用**
   - テスト完了後、mart.monthly_stock_valuation を v2 ベースに更新
