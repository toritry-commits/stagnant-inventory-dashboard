# BigQueryデータマッピング調査結果

**調査日**: 2025-12-25
**対象テーブル**: `clas-analytics.finance.fixed_asset_register`
**最新期**: term = 8 (2025-03-01 〜 2026-02-28)

---

## データ項目マッピング表

### 必須項目の存在確認

| Python項目 | 説明 | BigQueryカラム | 存在 | データ型 | 備考 |
|-----------|------|---------------|------|---------|------|
| 在庫id | 在庫ID | `stock_id` | ✅ | INTEGER | 既存 |
| パーツid | パーツID | `part_id` | ✅ | INTEGER | 既存 |
| パーツ名 | パーツ名 | `part_name` | ✅ | STRING | 既存 |
| **取得原価** | 購入価格 | `cost` | ✅ | INTEGER | 既存 |
| **耐用年数** | 償却期間（年） | `depreciation_period` | ✅ | INTEGER | 既存 |
| **入庫検品完了日** | 検品完了日 | `arrival_at` | ✅ | DATE | 既存（Pythonの`inspected_at`に相当） |
| 除売却日 | 除却・売却日 | `impossibility_date` | ✅ | TIMESTAMP | 既存 |
| 破損紛失分類 | 除却理由 | `stock_status_jp` | ✅ | STRING | 既存（「破損・紛失」など） |
| サプライヤー名 | 仕入先 | `supplier_name` | ✅ | STRING | 既存 |
| **減損損失日** | 減損計上日 | `impairment_date` | ✅ | DATE | **lake.stockに存在** |

### オプション項目の存在確認

| Python項目 | BigQueryカラム | 存在 | データ型 | 備考 |
|-----------|---------------|------|---------|------|
| sample | `sample` | ✅ | BOOLEAN | 既存 |
| 貸手リース開始日 | `lease_start_at` | ✅ | DATE | 既存 |
| 売却案件名 | `proposition_name` | ✅ | STRING | 既存（案件名） |
| 貸手リース案件名 | - | ❌ | - | 存在しない |
| 初回出荷日 | `first_shipped_at` | ✅ | DATE | 既存 |
| 売却日 | `sold_date` | ✅ | DATE | 既存 |

### 既に計算済みの項目

BigQueryの`fixed_asset_register`テーブルには、以下の計算済み項目が既に含まれています：

| カラム名 | 説明 | データ型 | Python対応項目 |
|---------|------|---------|---------------|
| `depreciation_months` | 償却月数 | INTEGER | 償却β相当 |
| `depreciation_beginning_months` | 期首償却月数 | INTEGER | 償却α相当 |
| `monthly_depreciation_amount` | 月次償却額 | FLOAT | 月次償却額 |
| `beginning_book_value` | 期首簿価 | FLOAT | 期首簿価 |
| `increase_value` | 増加額 | INTEGER | 増加取得原価 |
| `decrease_value` | 減少額 | INTEGER | 減少取得原価 |
| `accumulated_depreciation_amount` | 減価償却累計額 | FLOAT | 減価償却累計額 |
| `month_end_book_value` | 月末簿価 | FLOAT | - |
| `end_book_value` | 期末簿価 | FLOAT | 期末簿価 |

---

## カラム名の対応関係

### Python → BigQuery マッピング

| Pythonの列名 | BigQueryカラム | 変換メモ |
|-------------|---------------|---------|
| `在庫id` | `stock_id` | そのまま |
| `パーツid` | `part_id` | そのまま |
| `パーツ名` | `part_name` | そのまま |
| `取得原価` | `cost` | そのまま |
| `耐用年数` | `depreciation_period` | そのまま |
| `入庫検品完了日` | `arrival_at` | Pythonでは`inspected_at` |
| `供与開始日(初回出荷日)` | `first_shipped_at` | そのまま |
| `除売却日` | `impossibility_date` | TIMESTAMP型（Pythonでは正規化） |
| `破損紛失分類` | `stock_status_jp` | Pythonでは英語、BQでは日本語 |
| `サプライヤー名` | `supplier_name` | そのまま |
| `売却案件名` | `proposition_name` | そのまま |
| `貸手リース開始日` | `lease_start_at` | そのまま |
| `sample` | `sample` | BOOLEAN型 |
| `期首日(計算基準日)` | `beginning_term` | そのまま |
| `期末日(計算基準日)` | `end_term` | そのまま |

---

## データ品質確認

### サンプルデータ（stock_id = 3）

```
stock_id: 3
part_name: PB-「JF-5014」ちょうどソファ ヘッドレスト アースブラウン
category: Sofa
useful_life_years: 5
supplier_name: HONGKONG LETING FURNITURE CO.,LIMITED
acquisition_cost: 10281
inspected_at: 2018-08-29
first_shipped_at: 2018-10-14
impossibled_at: NULL (一部レコードでは 2025-09-23)
sample: false
stock_status_jp: 貸出可能 / 破損・紛失
start_date: 2025-03-01
end_date: 2026-02-28
```

### データの特徴

1. **同じstock_idで複数レコード**
   - term=8で stock_id=3 が複数行存在
   - `impossibility_date`の有無で異なる（履歴管理？）
   - 月次スナップショットの可能性

2. **破損紛失分類**
   - `stock_status_jp`に「破損・紛失」「貸出可能」「B向け貸出中」などの値
   - Pythonの`classification_of_impossibility`とは別の分類軸

3. **日付の形式**
   - `arrival_at`, `first_shipped_at`, `sold_date`, `lease_start_at`: DATE型
   - `impossibility_date`: TIMESTAMP型（時刻付き）

4. **カテゴリ情報**
   - `category`: "Sofa"などの商品カテゴリ
   - Pythonには存在しない追加情報

---

## 不足データへの対応方針

### ❌ 減損損失日（impairment_date）

**現状**: BigQueryテーブルに存在しない

**影響**:
- Pythonコードの減損損失計算ロジックが実装できない
- `期首/増加/減少/期末 減損損失累計額`の計算不可
- `期中減損損失`の計算不可

**対応案**:

#### 案1: 減損ロジックを実装する
```sql
-- 減損判定ロジック（例）
CASE
  WHEN DATE_DIFF(CURRENT_DATE(), first_shipped_at, MONTH) >= depreciation_period * 12
       AND end_book_value = 0
       AND impossibility_date IS NULL
  THEN first_shipped_at + INTERVAL (depreciation_period * 12) MONTH
  ELSE NULL
END AS impairment_date
```

#### 案2: 既存の簿価計算を活用する
- BigQueryテーブルに既に`end_book_value`が計算済み
- 減損損失の詳細は無視して、簿価=0のレコードを減損済みと判定
- Pythonの詳細ロジックは省略し、シンプルに実装

#### 案3: 減損損失日カラムを追加する
- `lake.stock`テーブルに`impairment_date`カラムを追加
- 手動またはバッチ処理で減損判定を実施
- 固定資産台帳の再構築

**推奨**: **案2（既存の簿価計算を活用）**
- 現在のBigQueryテーブルには既に簿価計算が存在
- Pythonの複雑な減損ロジックを再実装する必要性を確認すべき
- まずは既存データで十分か検証

---

## 既存テーブルとの関係性

### fixed_asset_register と lake.stock

```sql
-- 検証クエリ
SELECT
  s.id,
  s.cost as lake_cost,
  f.cost as far_cost,
  s.arrival_at as lake_arrival,
  f.arrival_at as far_arrival,
  s.inspected_at as lake_inspected,
  f.first_shipped_at as far_first_shipped,
  s.impossibled_at as lake_impossibled,
  f.impossibility_date as far_impossibility
FROM lake.stock s
LEFT JOIN finance.fixed_asset_register f ON s.id = f.stock_id
WHERE f.term = 8 AND s.id = 3
```

**結果**:
- `cost`は一致（10281円）
- `arrival_at`は一致（2018-08-29）
- `inspected_at` (lake) と `first_shipped_at` (far) は異なる日付
  - lake: 2018-08-29（入庫日）
  - far: 2018-10-14（初回出荷日）
- `impossibled_at`は最新のもの（2025-10-07）が反映されているが、
  固定資産台帳では履歴として複数レコードが存在

---

## 次のステップ

### 1. 既存簿価計算の検証

固定資産台帳に既に簿価計算が存在するため、Pythonのロジックと一致しているか検証：

```sql
-- stock_id=3の簿価計算を確認
SELECT
  stock_id,
  cost,
  depreciation_period,
  monthly_depreciation_amount,
  depreciation_beginning_months,
  depreciation_months,
  accumulated_depreciation_amount,
  beginning_book_value,
  end_book_value,
  beginning_term,
  end_term
FROM finance.fixed_asset_register
WHERE stock_id = 3 AND term = 8
```

### 2. Pythonとの計算結果比較

- Pythonで stock_id=3 の簿価を計算
- BigQueryの計算結果と比較
- 差異がある場合、原因を特定

### 3. 減損損失の扱いを決定

- 減損損失日が不要であれば、簿価=0を減損済みと扱う
- 減損損失日が必要であれば、データ追加またはロジック実装

### 4. v3の方針決定

**選択肢A**: 既存の固定資産台帳を活用
- `finance.fixed_asset_register`から簿価データを取得
- 滞留在庫ダッシュボードに統合

**選択肢B**: Pythonロジックを完全移植
- SQLでPythonの全ロジックを再実装
- 減損損失日などの不足データを補完

**選択肢C**: ハイブリッド
- 固定資産台帳の基礎データを使用
- 追加のビジネスロジック（減損判定など）をSQL実装

---

## データ調査のまとめ

### ✅ 存在するデータ

- ✅ 耐用年数（`depreciation_period`）
- ✅ 入庫検品完了日（`arrival_at`）
- ✅ 初回出荷日（`first_shipped_at`）
- ✅ サプライヤー名（`supplier_name`）
- ✅ 取得原価（`cost`）
- ✅ 既に計算済みの簿価（`end_book_value`）

### ❌ 存在しないデータ

- ❌ 減損損失日（`impairment_date`）

### 🔍 重要な発見

1. **BigQueryに既に簿価計算が存在**
   - `finance.fixed_asset_register`テーブルに期首簿価、期末簿価などが計算済み
   - Pythonのロジックを全て再実装する必要があるか要確認

2. **減損損失の扱い**
   - 減損損失日が存在しないため、Pythonの減損ロジックは実装できない
   - 既存の簿価計算で減損が考慮されているか検証が必要

3. **データの粒度**
   - 同じstock_idで複数レコードが存在（月次スナップショット？）
   - 履歴管理の仕組みを理解する必要あり
