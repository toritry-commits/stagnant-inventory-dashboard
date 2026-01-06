# 滞留在庫ダッシュボード

在庫管理・減損計算のためのBigQueryクエリ集とスプレッドシート自動更新スクリプトです。

## 概要

- **目的**: 減損対象となる在庫を事前に把握し、在庫回転施策を検討する
- **対象**: 365日以上滞留した在庫（期末に減損処理）
- **期末**: 2月末 / 5月末 / 8月末 / 11月末

## 成果物

### GASスクリプト（gas/）

| ファイル | 説明 |
|----------|------|
| `Code.js` | スプレッドシート自動更新スクリプト（毎朝9時自動実行） |
| `appsscript.json` | GASプロジェクト設定 |
| `.clasp.json` | clasp（デプロイツール）設定 |

**主な機能**:
- 毎朝9時にBigQueryからデータを自動取得・更新
- 表示モード切替（期末1のみ / 期末1・2のみ / 全期間）
- 手入力データの自動復元（SKU IDで紐づけ）
- 削除履歴シートへの自動保存

### 本番SQL（sql/）

| ファイル | 説明 |
|----------|------|
| `stagnant_inventory_dashboard_v2.sql` | 滞留在庫ダッシュボード（SKU単位で減損予定数・簿価を集計） |
| `select_fixed_asset_register_monthly.sql` | 固定資産台帳 月次簿価計算クエリ（2025年3月〜2027年2月） |
| `select_stock_valuation_summary.sql` | 在庫評価サマリークエリ |
| `term1_future_orders.sql` | 2026年2月末減損対象 × 未来案件紐づき在庫の抽出 |

### ドキュメント（docs/）

| カテゴリ | ファイル | 内容 |
|----------|----------|------|
| 要件定義 | `要件定義書.md` | 滞留在庫ダッシュボードの要件 |
| 要件定義 | `book_value_calculation_requirements.md` | 簿価計算の要件 |
| 設計書 | `stagnant_inventory_dashboard_v2_technical_spec.md` | 技術仕様書 |
| マニュアル | `user-manual.md` | 使い方ガイド（最新版） |
| お知らせ | `release-announcement.md` | リリースアナウンス文面 |

## フォルダ構成

```
stagnant-inventory-dashboard/
├── README.md                    # このファイル
├── gas/                         # GASスクリプト（スプレッドシート自動更新）
│   ├── Code.js
│   ├── appsscript.json
│   └── .clasp.json
├── sql/                         # 本番SQL
│   ├── stagnant_inventory_dashboard_v2.sql
│   ├── select_fixed_asset_register_monthly.sql
│   ├── select_stock_valuation_summary.sql
│   └── term1_future_orders.sql
├── docs/                        # ドキュメント
│   ├── 02_requirements/         # 要件定義
│   │   ├── 要件定義書.md
│   │   └── book_value_calculation_requirements.md
│   ├── 03_design/               # 設計書
│   │   └── stagnant_inventory_dashboard_v2_technical_spec.md
│   └── 04_user-manual/          # ユーザーマニュアル
│       ├── user-manual.md
│       └── release-announcement.md
├── tools/                       # ツール
│   ├── export_to_excel.py
│   └── export_with_overrides.py
└── archive/                     # アーカイブ
    ├── old_versions/            # 旧バージョンSQL
    ├── investigation/           # 調査資料
    └── stagnant-stock-report-backup/  # 旧リポジトリのバックアップ
```

## 使い方

### スプレッドシート（GAS）

スプレッドシートを開くと、メニューバーに「滞留在庫レポート」が表示されます。

| メニュー | 説明 |
|----------|------|
| 期末1の列だけ表示 | 次の期末に関する列のみ表示 |
| 期末1・2の列だけ表示 | 2期先までの列を表示 |
| すべての列を表示 | 全期間の列を表示 |
| データを今すぐ更新 | 最新データに手動更新 |

**自動更新**: 毎朝9時にBigQueryから最新データを取得して自動更新されます。

### BigQuery SQL

```bash
# 滞留在庫ダッシュボード
bq query --use_legacy_sql=false < sql/stagnant_inventory_dashboard_v2.sql

# 簿価計算
bq query --use_legacy_sql=false < sql/select_fixed_asset_register_monthly.sql

# 減損対象×未来案件紐づき
bq query --use_legacy_sql=false < sql/term1_future_orders.sql
```

## バージョン履歴

| 日付 | バージョン | 内容 |
|------|-----------|------|
| 2026-01-06 | 1.2.0 | GAS: 表示モード「期末1のみ」追加、メニュー構成刷新、デフォルト表示を期末1に変更 |
| 2025-12-26 | - | SQL: 簿価計算クエリ、2月末減損×未来案件クエリを追加 |
| 2025-12-24 | 1.0.0 | 滞留在庫ダッシュボードv2（排他カテゴリ方式、動的期末計算） |
| 2025-12-23 | - | 初版作成 |

## 関連ドキュメント

| ドキュメント | 対象者 | 内容 |
|---|---|---|
| [使い方ガイド](docs/04_user-manual/user-manual.md) | 業務担当者 | 目的別の活用方法、各項目の説明 |
| [技術仕様書](docs/03_design/stagnant_inventory_dashboard_v2_technical_spec.md) | 開発者 | CTE詳細解説、ロジック説明 |
| [要件定義書](docs/02_requirements/要件定義書.md) | 全員 | 要件と仕様 |
