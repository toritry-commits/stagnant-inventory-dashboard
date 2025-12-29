# BigQuery固定資産台帳データ調査サマリー

**調査日**: 2025-12-25
**調査者**: Claude Code

---

## 重要な発見

### 🎯 BigQueryに既に固定資産台帳が存在

テーブル: `clas-analytics.finance.fixed_asset_register`

このテーブルには、**既に簿価計算が完了したデータ**が格納されています。

```
最新期: term = 8 (2025-03-01 〜 2026-02-28)
総レコード数: 1,962,175件
ユニーク在庫数: 207,901件
```

---

## データ項目の存在確認

### ✅ すべての必須データが存在

| Python必須項目 | BigQueryカラム | 状態 |
|---------------|---------------|------|
| 耐用年数 | `depreciation_period` | ✅ 存在 |
| 入庫検品完了日 | `arrival_at` | ✅ 存在 |
| 初回出荷日 | `first_shipped_at` | ✅ 存在 |
| サプライヤー名 | `supplier_name` | ✅ 存在 |
| 取得原価 | `cost` | ✅ 存在 |

### ❌ 減損損失日のみ存在しない

| Python項目 | BigQueryカラム | 状態 |
|-----------|---------------|------|
| 減損損失日 | - | ❌ 存在しない |

---

## 既存の簿価計算の検証

### 計算ロジックの確認（stock_id=3の例）

```
【基本データ】
取得原価: 10,281円
耐用年数: 5年（60ヶ月）
初回出荷日: 2018-10-14
期首日: 2025-03-01
期末日: 2026-02-28

【計算結果】
月次償却額: 172円/月 (10,281 ÷ 60)
期首時点の経過月数: 78ヶ月 (2018-10 → 2025-03)
期末時点の経過月数: 83ヶ月 (2018-10 → 2026-02 + 1)
減価償却累計額: 14,276円 (172 × 83)
期末簿価: 0円 (10,281 - 14,276 = -3,995 → 0)
```

### 重要なポイント

1. **耐用年数超過時の処理**
   - 計算上の簿価がマイナスになる場合、0円に補正
   - これは減損損失ではなく、減価償却の上限処理

2. **減価償却累計額の上限**
   - 減価償却累計額は取得原価を超えない
   - 簿価は必ず0円以上

3. **Pythonロジックとの一致**
   - Pythonコードの`calculate_accumulated_depreciation_kimatsu()`と同じロジック
   - 耐用年数到達済みの場合、取得原価を返す（簿価=0）

---

## 減損損失の扱いについて

### 現状の理解

BigQueryテーブルには`impairment_date`（減損損失日）カラムが**存在しない**。

しかし、Pythonコードの減損ロジックを確認すると：

```python
def calculate_new_impairment_loss_kimatsu(row, end_date):
    # 減損損失日が期末以前の場合
    if pd.notna(impairment_date) and impairment_date <= end_date:
        book_value_kimatsu = acquisition_cost_kimatsu - accumulated_depreciation_kimatsu
        return max(0, book_value_kimatsu)
    return 0
```

**減損損失 = 減損時点の簿価**

つまり、減損を計上すると簿価が一気に0になる処理。

### 現在の固定資産台帳での対応

BigQueryの現在の簿価計算では、**減損損失は考慮されていない**可能性が高い。

代わりに：
- 耐用年数での通常の減価償却のみ
- 簿価=0になるのは耐用年数到達時のみ

---

## データマッピング完全版

| Python列名 | BigQueryカラム | 型 | 備考 |
|-----------|---------------|----|----|
| 在庫id | `stock_id` | INT | ✅ |
| パーツid | `part_id` | INT | ✅ |
| パーツ名 | `part_name` | STRING | ✅ |
| 取得原価 | `cost` | INT | ✅ |
| 耐用年数 | `depreciation_period` | INT | ✅（年単位） |
| 入庫検品完了日 | `arrival_at` | DATE | ✅（Pythonの`inspected_at`） |
| 供与開始日(初回出荷日) | `first_shipped_at` | DATE | ✅ |
| 除売却日 | `impossibility_date` | TIMESTAMP | ✅ |
| 破損紛失分類 | `stock_status_jp` | STRING | ✅（日本語表記） |
| サプライヤー名 | `supplier_name` | STRING | ✅ |
| 売却案件名 | `proposition_name` | STRING | ✅ |
| 貸手リース開始日 | `lease_start_at` | DATE | ✅ |
| 売却日 | `sold_date` | DATE | ✅ |
| sample | `sample` | BOOLEAN | ✅ |
| 期首日 | `beginning_term` | DATE | ✅ |
| 期末日 | `end_term` | DATE | ✅ |
| **減損損失日** | - | - | ❌ **存在しない** |

### 計算済み項目

| 計算項目 | BigQueryカラム | 型 |
|---------|---------------|----|
| 月次償却額 | `monthly_depreciation_amount` | FLOAT |
| 期首償却月数 | `depreciation_beginning_months` | INT |
| 期末償却月数 | `depreciation_months` | INT |
| 減価償却累計額 | `accumulated_depreciation_amount` | FLOAT |
| 期首簿価 | `beginning_book_value` | FLOAT |
| 期末簿価 | `end_book_value` | FLOAT |
| 増加額 | `increase_value` | INT |
| 減少額 | `decrease_value` | INT |

---

## 今後の方針（3つの選択肢）

### 選択肢A: 既存テーブルをそのまま活用 ⭐推奨

**概要**: `finance.fixed_asset_register`から簿価データを取得し、滞留在庫ダッシュボードに統合

**メリット**:
- ✅ 実装が最も簡単
- ✅ 既に検証済みの計算ロジック
- ✅ 減損損失日がなくても動作

**デメリット**:
- ❌ 減損損失の詳細が追跡できない
- ❌ Pythonの細かいロジック（リース再取得など）が反映されない可能性

**実装方針**:
```sql
-- 滞留在庫ダッシュボードv3
WITH stock_with_book_value AS (
  SELECT
    s.*,
    f.end_book_value,
    f.depreciation_period,
    f.accumulated_depreciation_amount
  FROM lake.stock s
  LEFT JOIN finance.fixed_asset_register f
    ON s.id = f.stock_id
   AND f.term = (SELECT MAX(term) FROM finance.fixed_asset_register)
)
-- 既存のCTEと組み合わせ
```

---

### 選択肢B: Pythonロジックを完全移植

**概要**: Pythonの全ロジックをBigQuery SQLに移植し、独自に簿価を計算

**メリット**:
- ✅ Pythonと完全に同じロジック
- ✅ 減損損失日を追加すれば完全実装可能
- ✅ カスタマイズ性が高い

**デメリット**:
- ❌ 実装が複雑（30+個の関数をSQL化）
- ❌ 減損損失日データの準備が必要
- ❌ テストと検証に時間がかかる

**必要な作業**:
1. 減損損失日のデータ追加（手動 or ロジック実装）
2. Python関数を全てCTEに変換
3. 具体的なデータで検証
4. パフォーマンス最適化

---

### 選択肢C: ハイブリッド（部分移植）

**概要**: 固定資産台帳の基礎データを使いつつ、不足部分をSQL補完

**メリット**:
- ✅ 既存データを活用しつつカスタマイズ可能
- ✅ 減損ロジックなど必要な部分だけ実装

**デメリット**:
- ❌ 二重管理のリスク
- ❌ データ整合性の確認が必要

**実装例**:
```sql
WITH base_data AS (
  SELECT * FROM finance.fixed_asset_register
  WHERE term = (SELECT MAX(term) FROM finance.fixed_asset_register)
),
-- 減損ロジックを追加
impairment_logic AS (
  SELECT *,
    CASE
      WHEN end_book_value = 0 AND impossibility_date IS NOT NULL
      THEN impossibility_date
      ELSE NULL
    END AS estimated_impairment_date
  FROM base_data
)
```

---

## 推奨アクション

### 🎯 まずは選択肢Aで進める

理由：
1. **既存の固定資産台帳が信頼できるデータ**である可能性が高い
2. 減損損失日がなくても、簿価計算は可能
3. 最小の工数で「取得原価→簿価」への移行が完了

### 次のステップ

1. **既存テーブルの計算ロジック確認**
   - どのように簿価が計算されているか、元のクエリを確認
   - Pythonのロジックとの差異を特定

2. **滞留在庫ダッシュボードv3の実装**
   - `finance.fixed_asset_register`とJOIN
   - `cost`を`end_book_value`に置き換え

3. **差分があれば調整**
   - 減損損失が必要なケースを特定
   - 必要に応じてロジック追加

---

## 質問事項（ユーザー確認が必要）

### Q1: 減損損失の重要性

現在のビジネス要件において、**減損損失の詳細な追跡は必須**ですか？

- **はい** → 選択肢Bまたは選択肢Cで減損ロジック実装が必要
- **いいえ** → 選択肢Aで十分（簿価=0を減損済みと扱う）

### Q2: 既存の固定資産台帳の信頼性

`finance.fixed_asset_register`テーブルの簿価計算は、現在の会計処理で使用されていますか？

- **はい** → 選択肢Aが最適（既存データを信頼）
- **いいえ** → 選択肢Bで新規実装が必要

### Q3: Pythonアプリの使用状況

現在のPythonアプリ（`python_book_value_logic.py`）は：
- A. 実際の会計処理で使用中
- B. 試験的に使用
- C. これから導入予定

→ Aの場合、Pythonとの完全一致が必要（選択肢B）

---

## まとめ

### 調査結果

✅ **必要なデータはすべて存在**（減損損失日を除く）
✅ **既に簿価計算済みのテーブルが存在**
❌ **減損損失日のみ存在しない**

### 推奨事項

**選択肢A（既存テーブル活用）** から始めて、必要に応じて拡張するアプローチが最も効率的です。
