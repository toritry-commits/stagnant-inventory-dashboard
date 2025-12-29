# BigQuery SQL実装完了報告

**作成日**: 2025-12-26
**対象期間**: 2025年3月1日（期首）〜 2025年11月30日（期末）
**ステータス**: ✅ SQL実装完了（Phase 1〜8）

---

## 📁 作成されたファイル

### 1. メインSQL実装

| ファイル名 | 用途 | 説明 |
|-----------|------|------|
| [fixed_asset_register_sql.sql](fixed_asset_register_sql.sql) | 完全版クエリ | 全8層のCTEを含む完全なSQLクエリ（SELECT文） |
| [create_table_20251130.sql](create_table_20251130.sql) | テーブル作成 | `CREATE OR REPLACE TABLE`でテーブルを作成するクエリ |
| [test_stock_id_3.sql](test_stock_id_3.sql) | 単体テスト | stock_id=3での検証用クエリ（Level 0〜2のみ） |

---

## 🏗️ 実装したCTE構造（全8層）

### Phase 1: 基礎データ層

#### Level 0: `base_stock_data`
- 4テーブルのJOIN（stock, part, supplier, stock_acquisition_costs, fixed_asset_register）
- 取得原価の計算: `cost + overhead_cost - discount`
- 月次償却額の計算（耐用年数0年対応）

#### Level 1: `enriched_data`
- 契約識別コードの抽出: `REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$')`
- リース再取得日の計算（2パターン）
  - 株式会社カンム: `2024-[月]-1`
  - 三井住友トラスト・パナソニックファイナンス: 契約開始から30ヶ月後の月末
- 初回出荷日の計算（2パターン + デフォルト）

---

### Phase 2: 償却期間層

#### Level 2: `depreciation_periods`
- 償却α（期首時点償却済み月数）
- 償却β（期末時点償却済み月数）
- 償却γ（リース再取得時点償却済み月数）
- 各償却期間で減損・貸手リース・除却の優先順位を実装

---

### Phase 3: 取得原価層

#### Level 3: `acquisition_costs`
- レベシェア判定フラグ: `is_reveshare`
- 期首取得原価（リース再取得日判定含む）
- 増加取得原価（リース再取得 or 新規入庫）
- 減少取得原価
- 期末取得原価（リース再取得日判定含む）

---

### Phase 4: 償却月数層

#### Level 4: `amortization_months`
- 期首償却月数
- **償却償却月数（期中償却月数）**
  - ★重要: β-γ vs β-α の切り替えロジック実装
  - リース再取得日が期首〜期末の間なら `β - γ`
  - それ以外は `β - α`
- 増加償却月数
- 減少償却月数
- 期末償却月数

---

### Phase 5: 減価償却累計額層

#### Level 5: `accumulated_depreciation`
- 期首減価償却累計額（リース再取得日判定含む）
- 増加減価償却累計額
- 減少減価償却累計額
- 期中減価償却費（複雑なロジック）
- 期末減価償却累計額

---

### Phase 6: 簿価仮計算層（循環参照回避）

#### Level 6: `book_values_temp`
- 仮期首簿価: `acquisition_cost_opening - accumulated_depreciation_opening`
- 仮増加簿価: `acquisition_cost_increase - accumulated_depreciation_increase`
- **目的**: 増加減損損失累計額の計算で循環参照を回避

---

### Phase 7: 減損損失累計額層

#### Level 7: `impairment_losses`
- 期首減損損失累計額
- **増加減損損失累計額（修正版ロジック使用）**
  - `temp_book_value_opening + temp_book_value_increase - interim_depreciation_expense`
- 減少減損損失累計額
- 期中減損損失
- 期末減損損失累計額

---

### Phase 8: 最終簿価層

#### Level 8: `final_book_values`
- 期首簿価: `acquisition_cost_opening - accumulated_depreciation_opening - impairment_loss_opening`
- 増加簿価: `acquisition_cost_increase - accumulated_depreciation_increase - impairment_loss_increase`
- 減少簿価: `acquisition_cost_decrease - accumulated_depreciation_decrease - impairment_loss_decrease`
- 期末簿価: `acquisition_cost_closing - accumulated_depreciation_closing - impairment_loss_closing`

---

## 📊 出力テーブルスキーマ

### テーブル名
```
clas-analytics.finance.fixed_asset_register_sql_20251130
```

### カラム構成（42カラム）

| カテゴリ | カラム数 | カラム名 |
|---------|---------|---------|
| 識別情報 | 4 | stock_id, part_id, part_name, supplier_name |
| 基礎データ | 3 | actual_cost, depreciation_period, monthly_depreciation |
| 日付情報 | 6 | inspected_at, first_shipped_at, lease_reacquisition_date, impossibled_at, impairment_date, lease_start_at |
| フラグ・分類 | 2 | sample, classification_of_impossibility |
| 償却期間 | 3 | shokyaku_alpha, shokyaku_beta, shokyaku_gamma |
| 取得原価 | 4 | acquisition_cost_opening, increase, decrease, closing |
| 償却月数 | 5 | amortization_months_opening, depreciation, increase, decrease, closing |
| 減価償却累計額 | 5 | accumulated_depreciation_opening, increase, decrease, interim_expense, closing |
| 減損損失累計額 | 5 | impairment_loss_opening, increase, decrease, interim, closing |
| **簿価** ★ | 4 | **book_value_opening, increase, decrease, closing** |
| 固定値 | 2 | period_start, period_end |
| **合計** | **42** | |

---

## ✅ 実装済み重要ロジック

### 1. レベシェア判定（全計算項目で使用）
```sql
(REGEXP_CONTAINS(supplier_name, 'レベシェア')
 AND NOT REGEXP_CONTAINS(supplier_name, 'リース・レベシェア'))
OR REGEXP_CONTAINS(supplier_name, '法人小物管理用')
OR sample
```

### 2. リース再取得日の計算（2パターン）
- **株式会社カンム**: 契約No.末尾数字 → `2024-[月]-1`
- **三井住友トラスト・パナソニックファイナンス**: 契約識別コード → 契約開始から30ヶ月後の月末

### 3. 償却償却月数のβ-γ vs β-α切り替え
```sql
WHEN DATE_TRUNC(lease_reacquisition_date, MONTH) >= period_start
THEN LEAST(depreciation_period * 12, shokyaku_beta)
     - LEAST(depreciation_period * 12, shokyaku_gamma)
ELSE LEAST(depreciation_period * 12, shokyaku_beta)
     - LEAST(depreciation_period * 12, shokyaku_alpha)
```

### 4. 減損損失の優先順位（償却α/β/γ共通）
1. 減損損失日
2. 貸手リース開始日
3. 除却日（庫内紛失／棚卸差異は+1ヶ月）
4. 除却日（通常）
5. デフォルト（期首日/期末日/リース再取得日）

### 5. 期中減価償却費の複雑なロジック
```sql
WHEN actual_cost <= depreciation_period * 12
     AND actual_cost > shokyaku_alpha
     AND actual_cost < shokyaku_beta
THEN actual_cost - shokyaku_alpha
WHEN actual_cost < LEAST(shokyaku_alpha, depreciation_period * 12) THEN 0
WHEN depreciation_period * 12 <= shokyaku_beta
THEN actual_cost - monthly_depreciation * (depreciation_period * 12 - 1)
     + monthly_depreciation * (amortization_months_depreciation - 1)
ELSE monthly_depreciation * amortization_months_depreciation
```

---

## 🧪 次のステップ（Phase 9）

### ステップ9-1: 小規模テスト実行

1. **stock_id=3での検証**
```sql
-- test_stock_id_3.sql を実行
-- 期待結果:
-- - actual_cost: 10281
-- - monthly_depreciation: 171.35
-- - shokyaku_alpha: 77
-- - shokyaku_beta: 86
-- - shokyaku_gamma: 0
```

2. **Pythonとの比較**
   - stock_id=3の全42カラムを比較
   - 許容誤差: ±0.01円

---

### ステップ9-2: テーブル作成

1. **テーブル作成クエリの実行**
```bash
# BigQueryコンソールで以下のファイルを実行
create_table_20251130.sql
```

2. **実行時間の測定**
   - 期待: 10分以内（20万件）

3. **レコード数の確認**
```sql
SELECT COUNT(*) FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`;
-- 期待: 約20万件
```

---

### ステップ9-3: Pythonとの比較検証

1. **ランダム10件での比較**
```sql
SELECT *
FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`
WHERE stock_id IN (
  SELECT stock_id
  FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`
  ORDER BY RAND()
  LIMIT 10
);
```

2. **簿価の合計値比較**
```sql
SELECT
  SUM(book_value_opening) AS total_opening,
  SUM(book_value_closing) AS total_closing
FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`;
```

3. **データ品質チェック**
```sql
-- 簿価の整合性チェック
SELECT
  stock_id,
  book_value_opening + book_value_increase - book_value_decrease AS calculated_closing,
  book_value_closing,
  ABS((book_value_opening + book_value_increase - book_value_decrease) - book_value_closing) AS diff
FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`
WHERE ABS((book_value_opening + book_value_increase - book_value_decrease) - book_value_closing) > 0.01
LIMIT 10;
```

---

## 📋 成功基準

### 必須条件
- ✅ stock_id=3でPythonと完全一致（±0.01円）
- ✅ ランダム10件で全項目が一致
- ✅ 実行時間10分以内

### データ品質
- ✅ 簿価の整合性: `期首+増加-減少=期末`（誤差0.01円以内）
- ✅ 減価償却累計額 ≤ 取得原価
- ✅ NULL値がない（意図的なNULL除く）

---

## 📝 実装時の注意点

### Looker Studio → BigQuery の関数変換

| Looker Studio | BigQuery | 備考 |
|--------------|----------|------|
| `DATETIME_TRUNC(date, MONTH)` | `DATE_TRUNC(date, MONTH)` | DATE型で統一 |
| `NARY_MAX(a, b)` | `GREATEST(a, b)` | 最大値取得 |
| `NARY_MIN(a, b)` | `LEAST(a, b)` | 最小値取得 |
| `YEAR(date)` | `EXTRACT(YEAR FROM date)` | 年抽出 |
| `MONTH(date)` | `EXTRACT(MONTH FROM date)` | 月抽出 |
| `IF(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` | 条件分岐 |

---

## 🔄 今後の拡張（Phase 10以降）

### 多期間実装への拡張
- 計算期間マスタテーブルの作成
- 13ヶ月分のCROSS JOIN
- 月次バッチ実行スケジュール
- テーブル名: `book_value_calculation`（260万件）

---

## 📌 重要な参照ドキュメント

| ドキュメント | 内容 |
|------------|------|
| [fixed_period_implementation_plan.md](fixed_period_implementation_plan.md) | 実装計画書（Phase 1〜9の詳細） |
| [looker_studio_formulas.md](looker_studio_formulas.md) | Looker Studioの計算式（ソース） |
| [final_data_checklist.md](final_data_checklist.md) | 必要データ項目の最終チェックリスト |
| [lease_reacquisition_impact_analysis.md](lease_reacquisition_impact_analysis.md) | リース再取得の影響分析 |

---

**実装完了日**: 2025-12-26
**次のアクション**: Phase 9の実行（BigQueryでのテスト実行とPython比較検証）

---

## ✅ Phase 1〜8 実装完了

すべてのCTE層のSQL実装が完了しました。次はBigQueryでクエリを実行し、Pythonの固定資産台帳との比較検証を行ってください。
