# 余りパーツ「解消提案」レポート 要件定義

作成日: 2026-07-29
ステータス: 要件確定 (実装前)

---

## 1. 目的

余り在庫IDの「数」を見るだけでなく、以下を見える化して購買判断の材料にする。

- なぜ余っているのか (どの相方パーツが足りないのか)
- いくら投資すれば余りを商品に変えられるか

## 2. 軸

在庫ID軸 -> **SKU x エリア軸** に変更する。

理由: 余りの帰属は SP のロジック上「そのパーツを最も使う SKU (貸出回数トップ)」に紐づいており、共通パーツ問題は SKU 軸でしか正しく表現できない。

## 3. 出力レイアウト (2026-07-29 改訂)

1行 = SKU x エリア x 不足している相方パーツ (21列)

| 列グループ | 列 | 出所 |
|-----------|----|------|
| SKU表示情報 | 画像 (=IMAGE), SKU_ID, 商品ID, 商品IDリンク, シリーズ名, 商品名, 属性, カテゴリ, サプライヤー | faithful版 Section 12 流用 (series/attribute/image/part_version+supplier) |
| 余り状況 | エリア, 余りパーツ内訳 (パーツごとセル内改行), 余り点数_貸出可能 | 既存 leftover ロジック |
| 解消提案 | 追加で組める商品数, 相方パーツID, 相方パーツ名, 必要点数_1商品あたり, 相方の余り在庫, 追加必要点数, 単価, 必要金額 | 計算 |
| 参考 | 相方の管理対象外内訳 (Impossibility を分類タグ別にセル内改行、全エリア合算) | stock.classification_of_impossibility |

- シート非表示 (テーブルのみ保持): 単価出所, SKU合計必要金額 (並び順に使用)
- 削除した列: 参考_余り点数_リカバリー中 / 発注済未着 (ユーザー指示)
- 画像URLは「拡張子なし + resize.width=1080&...&format=webp」形式 (実績フォーマット。faithful版の type=Crop 形式は404になる)

## 4. 計算ロジック

例: SKU = パーツA x2 + パーツB x1、関東に A が4点余り、B が0点の場合

- 追加で組める商品数の上限 = floor(4 / 2) = 2商品 (余りAを使い切る数)
- 相方 B の必要数 = 1点 x 2セット = 2点
- 必要金額 = B の単価 x 2点

## 5. 確定した仕様判断

| 論点 | 決定 | 理由 |
|------|------|------|
| セット数の上限 | 余りパーツを使い切る数 | 余りが尽きた先は新規増産でありレポート範囲外。自然に有限になる |
| 在庫価格の定義 | 相方パーツの既存在庫 cost 平均 | 実際の仕入額ベースで手堅い |
| 相方が複数ある場合 | 相方パーツごとに1行。SKUレベルの組める商品数は全相方を揃えた場合の値 | - |
| 余りのステータス | 貸出可能のみを「使える余り」として計算。リカバリー中・発注済未着は参考列 | 今すぐ組めるかの判断が目的 |

## 6. 出力先と実装 (2026-07-29 実装完了)

既存スプレッドシート「無題のスプレッドシート」のシート「余りパーツ解消提案」に出力。

- URL: https://docs.google.com/spreadsheets/d/1luwTZexd-jXGTlrXF8K1pmIeeVRUsdI49PZnGe7id2o/
- パイプライン: `sql/leftover_reason_report.sql` (bq query) -> `clas-analytics.sandbox.leftover_reason_report` テーブル -> GAS `writeLeftoverReport` -> シート
- GAS: `gas-output-sheet/` (スプシ bound スクリプト、clasp run 対応済み)

### 2シート構成

| シート | 粒度 | 元テーブル (sandbox) | GAS関数 |
|--------|------|---------------------|---------|
| 余りパーツ解消提案 | SKU x エリア x 相方パーツ (698行) | leftover_reason_report | writeLeftoverReport |
| 余りパーツ解消提案_SKU単位 | SKU x エリア (436行、相方はセル内改行) | leftover_reason_report_by_sku | writeLeftoverReportBySku |

### 更新手順

```bash
# 1. テーブル再作成 (ライブ値なので実行のたびに変わる)
cd stagnant-inventory-dashboard/sql
bq query --use_legacy_sql=false --location=asia-northeast1 --project_id=clas-analytics < leftover_reason_report.sql
bq query --use_legacy_sql=false --location=asia-northeast1 --project_id=clas-analytics < leftover_reason_report_by_sku.sql

# 2. スプシに反映
cd ../gas-output-sheet
clasp run writeLeftoverReport
clasp run writeLeftoverReportBySku
```

### 実装時の追加判断 (事後報告)

| 論点 | 決定 | 理由 |
|------|------|------|
| 単価のフォールバック | stock.cost 平均 -> purchasing_detail.cost 平均 (単価であることを実データで照合済み) | 不足パーツは在庫ゼロが多く stock.cost だけでは半数近く単価が取れない |
| 単価出所列を追加 | '在庫cost平均' / '発注cost平均' / 空欄 | 金額の信頼度を判別できるように |
| 単価なしの扱い | 空欄のまま出力 (698行中302行、うち123行は法人パーツ) | どこにも原価データが無い。捏造しない |

## 7. ベースとなる既存資産

- `sql/leftover_id_count_standalone.sql` — mart 非依存の余り計算 (推奨ベース)
- `sql/leftover_id_count_faithful.sql` — SP 丸写し検証版
- 由来: `order-quantity-dashboard/sql/mart/sp_sku_inventory_v2.sql`

## 8. 注意事項

- lake.stock を日付ピンなしで読むため実行のたびに数値が変わる (ライブ値)
- 共通パーツは top SKU (貸出回数最多) に帰属させる既存ロジックを踏襲する。相方パーツの購入が他 SKU にも効く場合の重複計上には注意 (v1 では許容)
