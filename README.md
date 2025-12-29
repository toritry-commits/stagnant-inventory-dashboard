# 滞留在庫ダッシュボード

在庫管理・減損計算のためのBigQueryクエリ集です。

## 概要

- **目的**: 減損対象となる在庫を事前に把握し、在庫回転施策を検討する
- **対象**: 365日以上滞留した在庫（期末に減損処理）
- **期末**: 2月末 / 5月末 / 8月末 / 11月末

## 成果物

### 本番SQL（sql/）

| ファイル | 説明 |
|----------|------|
| `stagnant_inventory_dashboard_v2.sql` | 滞留在庫ダッシュボード（SKU単位で減損予定数・取得原価を集計） |
| `select_fixed_asset_register_monthly.sql` | 固定資産台帳 月次簿価計算クエリ（2025年3月〜2027年2月） |
| `term1_future_orders.sql` | 2026年2月末減損対象 × 未来案件紐づき在庫の抽出 |

### ドキュメント（docs/）

| カテゴリ | ファイル | 内容 |
|----------|----------|------|
| 要件定義 | `要件定義書.md` | 滞留在庫ダッシュボードの要件 |
| 要件定義 | `book_value_calculation_requirements.md` | 簿価計算の要件 |
| 設計書 | `stagnant_inventory_dashboard_v2_technical_spec.md` | 技術仕様書 |
| マニュアル | `stagnant_inventory_dashboard_v2_user_manual.md` | 使い方ガイド |

## フォルダ構成

```
stagnant-inventory-dashboard/
├── README.md                    # このファイル
├── sql/                         # 本番SQL
│   ├── stagnant_inventory_dashboard_v2.sql
│   ├── select_fixed_asset_register_monthly.sql
│   └── term1_future_orders.sql
├── docs/                        # ドキュメント
│   ├── 02_requirements/
│   ├── 03_design/
│   └── 04_user-manual/
└── archive/                     # アーカイブ
    ├── old_versions/            # 旧バージョン
    └── investigation/           # 調査資料
```

## 使い方

### 1. 滞留在庫ダッシュボード

```bash
bq query --use_legacy_sql=false < sql/stagnant_inventory_dashboard_v2.sql
```

SKU単位で各期末の減損予定数・取得原価を算出します。

### 2. 簿価計算

```bash
bq query --use_legacy_sql=false < sql/select_fixed_asset_register_monthly.sql
```

在庫ごとに月次の簿価情報を計算します。Looker Studioなどで任意の期間を指定して簿価集計を行うためのハイブリッド設計（月次増減 + 累計値）を採用しています。

### 3. 減損対象×未来案件紐づき

```bash
bq query --use_legacy_sql=false < sql/term1_future_orders.sql
```

2026年2月末で減損対象となる在庫のうち、3月以降に配送予定の法人案件に紐づいているものを抽出します。監査法人との協議資料として使用します。

## バージョン履歴

| 日付 | 内容 |
|------|------|
| 2025-12-26 | 簿価計算クエリ、2月末減損×未来案件クエリを追加 |
| 2025-12-24 | 滞留在庫ダッシュボードv2（排他カテゴリ方式、動的期末計算、取得原価カラム追加） |
| 2025-12-23 | 初版作成 |

## 関連ドキュメント

| ドキュメント | 対象者 | 内容 |
|---|---|---|
| [使い方ガイド](docs/04_user-manual/stagnant_inventory_dashboard_v2_user_manual.md) | 業務担当者 | 目的別の活用方法、各項目の説明 |
| [技術仕様書](docs/03_design/stagnant_inventory_dashboard_v2_technical_spec.md) | 開発者 | CTE詳細解説、ロジック説明 |
| [要件定義書](docs/02_requirements/要件定義書.md) | 全員 | 要件と仕様 |
