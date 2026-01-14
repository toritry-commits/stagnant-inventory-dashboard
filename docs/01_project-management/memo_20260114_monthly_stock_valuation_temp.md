# 2025年12月 臨時簿価計算クエリ作成メモ

**作成日:** 2026-01-14
**目的:** BigQueryに未反映のデータを上書きして、2025年12月の簿価を正しく計算する

---

## 背景

2025年12月分の簿価計算において、BigQuery(lake)に反映されていないデータがあったため、一時テーブルを使って上書きする臨時クエリを作成した。

---

## 作成したファイル

### SQLクエリ
- `sql/monthly_stock_valuation_v2_202512_temp.sql`
  - 元ファイル `monthly_stock_valuation_v2.sql` をコピーして作成
  - 対象期間を 2025年12月1日〜2025年12月31日 に変更
  - 3つの上書きテーブルを参照するように修正

### BigQuery一時テーブル (sandbox)

| テーブル名 | 用途 | カラム |
|-----------|------|--------|
| `sold_override_202512` | 法人売却の上書き | stock_id, impossibled_at, sold_proposition_name |
| `cost_override_202512` | 取得原価の上書き | id(=stock_id), cost, overhead_cost, discount |
| `smtpf_override_202512` | SMTPFリースバック品の上書き | stock_id, cost, supplier_id |

### CSVデータ保存先
- `data/202512_temp/sold_20260114.csv`

---

## 上書き処理の詳細

### 1. 売却データ上書き (sold_override_202512)

CSVにある売却データでBigQuery未反映分を上書き。

| 項目 | 処理 |
|------|------|
| `impossibled_at` | CSVの除売却日を優先 |
| `classification_of_impossibility` | CSVにデータがあれば `SoldToBusiness` (法人売却) |
| `sold_proposition_name` | CSVの売却案件名を優先 |

### 2. コストデータ上書き (cost_override_202512)

| 項目 | 処理 |
|------|------|
| `actual_cost` | `cost + overhead_cost - discount` で計算 |
| `monthly_depreciation` | 上書きされた取得原価から再計算 |

**注意:** `overhead_cost` と `discount` はSTRING型のため、クエリ内で `SAFE_CAST` でINT64に変換している

### 3. SMTPFリースバック上書き (smtpf_override_202512)

三井住友トラスト・パナソニックファイナンスのリースバック品について、サプライヤーIDとコストを上書き。

| 項目 | 処理 |
|------|------|
| `supplier_name` | supplier_idからsupplierテーブルを参照して名前を取得 |
| `contract_code` | 上書きされたサプライヤー名から契約開始コードを抽出 |
| `actual_cost` | smtpf > cost_override > 元データの優先順位 |
| `monthly_depreciation` | 上書きされた取得原価から再計算 |

**影響:** サプライヤー名が変わることで、以下の計算が変わる
- `is_smtpf_pattern` (SMTPFパターン判定)
- `calc_lease_reacquisition_date` (リース再取得日)
- `calc_first_shipped_at` (初回出荷日)
- `adjust_lease_start_at` (リース開始日調整)

---

## 優先順位

取得原価の上書きには優先順位がある:

```
smtpf_override.cost > cost_override(cost + overhead_cost - discount) > 元データ
```

競合チェック済み: `smtpf_override_202512` と `cost_override_202512` に同じstock_idは存在しない

---

## クエリの実行方法

1. BigQueryコンソールで `monthly_stock_valuation_v2_202512_temp.sql` を実行
2. 結果をエクスポートして利用

---

## 注意事項

- このクエリは臨時用。本番の `monthly_stock_valuation_v2.sql` には影響しない
- sandboxの一時テーブルは不要になったら削除すること
- CSVアップロード時は文字コードに注意(UTF-8推奨)

---

## 棚卸資産振替日 (inventory_transfer_date)

外部販売の承認日を棚卸資産振替日として出力に追加。

### データソース
- `lake.external_sale_stock` と `lake.external_sale_product` をJOIN

### 取得ロジック

```sql
CASE
  WHEN esp.status IN ('Sellable', 'Soldout') THEN
    CASE
      WHEN DATE(esp.created_at) = DATE(2025, 3, 24) THEN esp.created_at
      WHEN DATE(esp.created_at) = DATE(2025, 4, 8) THEN esp.created_at
      ELSE esp.authorized_at
    END
  ELSE NULL
END
```

### データ検証結果
- 全データ数: **863件** (2025/3/24 - 2026/1/8)
- 2025年12月: **120件**
