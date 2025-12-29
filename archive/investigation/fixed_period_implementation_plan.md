# 固定資産台帳SQL実装計画（固定期間版）

**作成日**: 2025-12-26
**目的**: Pythonで出力される固定資産台帳と同じものをBigQuery SQLで再現
**対象期間**: 2025年3月1日（期首）〜 2025年11月30日（期末）

---

## 📋 実装方針の変更点

### 当初の計画からの変更
- ❌ **方式B（13ヶ月分の事前計算）** → 後回し
- ✅ **方式A'（固定期間の単一計算）** → 最初に実装

### 変更理由
1. Pythonの固定資産台帳との完全一致を最優先
2. 計算ロジックの検証を容易にする
3. 複数期間の実装は検証完了後に追加

---

## 🎯 実装目標

### 出力イメージ
Pythonで出力される固定資産台帳（Excel/CSV）と同じ構造のテーブルを作成:

| stock_id | part_name | 期首取得原価 | 増加取得原価 | 減少取得原価 | 期末取得原価 | ... | 期首簿価 | 増加簿価 | 減少簿価 | 期末簿価 |
|----------|-----------|------------|------------|------------|------------|-----|---------|---------|---------|---------|
| 3 | ちょうどソファ... | 10281 | 0 | 0 | 10281 | ... | 8500 | 0 | 0 | 8300 |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

**出力形式**: 横持ち（1行に全項目）
**対象レコード数**: 約20万件（全在庫）
**期間**: 固定（2025-03-01 〜 2025-11-30）

---

## 📅 固定パラメータ

```sql
-- 固定値として定義
DECLARE period_start DATE DEFAULT DATE '2025-03-01';  -- 期首日
DECLARE period_end DATE DEFAULT DATE '2025-11-30';    -- 期末日
```

**注意点**:
- 期首日は常に会計年度の開始日（3月1日）
- 期末日は計算対象月の月末（11月30日）

---

## 🏗️ SQL実装の構造

### CTE（Common Table Expression）の階層

```sql
-- 固定パラメータの定義
DECLARE period_start DATE DEFAULT DATE '2025-03-01';
DECLARE period_end DATE DEFAULT DATE '2025-11-30';

WITH
-- レベル0: 基礎データの取得とJOIN
base_stock_data AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    p.name AS part_name,
    sup.name AS supplier_name,
    s.sample,
    s.inspected_at,
    s.impossibled_at,
    s.impairment_date,
    s.classification_of_impossibility,

    -- 取得原価の計算（cost + overhead_cost - discount）
    COALESCE(s.cost, 0)
      + COALESCE(sac.overhead_cost, 0)
      - COALESCE(sac.discount, 0) AS actual_cost,

    -- 耐用年数
    COALESCE(far.depreciation_period, 0) AS depreciation_period,

    -- 初回出荷日（fixed_asset_registerから）
    far.first_shipped_at AS first_shipped_at_base,

    -- 貸手リース開始日
    far.lease_start_at,

    -- 月次償却額の計算
    CASE
      WHEN COALESCE(far.depreciation_period, 0) = 0 THEN 0
      ELSE (COALESCE(s.cost, 0)
            + COALESCE(sac.overhead_cost, 0)
            - COALESCE(sac.discount, 0)) / (far.depreciation_period * 12)
    END AS monthly_depreciation

  FROM `clas-analytics.lake.stock` s
  LEFT JOIN `clas-analytics.lake.part` p ON s.part_id = p.id
  LEFT JOIN `clas-analytics.lake.supplier` sup ON s.supplier_id = sup.id
  LEFT JOIN `clas-analytics.lake.stock_acquisition_costs` sac ON s.id = sac.stock_id
  LEFT JOIN (
    SELECT stock_id, depreciation_period, first_shipped_at, lease_start_at
    FROM `clas-analytics.finance.fixed_asset_register`
    WHERE term = (SELECT MAX(term) FROM `clas-analytics.finance.fixed_asset_register`)
  ) far ON s.id = far.stock_id
  WHERE s.deleted_at IS NULL
),

-- レベル1: 契約識別コード、リース再取得日、初回出荷日の計算
enriched_data AS (
  SELECT
    *,
    -- 契約識別コードの抽出
    REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$') AS contract_code,

    -- リース再取得日の計算
    CASE
      -- 株式会社カンム パターン（2024年の該当月の1日）
      WHEN REGEXP_CONTAINS(supplier_name, r'^株式会社カンム 契約No\.2022000(\d)$')
      THEN PARSE_DATE('%Y-%m-%d',
        CONCAT('2024-', REGEXP_EXTRACT(supplier_name, r'契約No\.2022000(\d)'), '-1'))

      -- 三井住友トラスト・パナソニックファイナンス パターン
      WHEN STARTS_WITH(supplier_name, '三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始')
           AND REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$') IS NOT NULL
           AND LENGTH(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$')) = 4
      THEN DATE_SUB(
        DATE_TRUNC(
          DATE_ADD(
            DATE_ADD(
              DATE(
                CAST(CONCAT('20', SUBSTR(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$'), 1, 2)) AS INT64),
                CAST(SUBSTR(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$'), 3, 2) AS INT64),
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
    END AS lease_reacquisition_date,

    -- 初回出荷日の計算（サプライヤー名パターン or デフォルト値）
    CASE
      -- 株式会社カンム パターン
      WHEN REGEXP_CONTAINS(supplier_name, r'^株式会社カンム 契約No\.2022000(\d)$')
      THEN PARSE_DATE('%Y-%m-%d',
        CONCAT('2022-', REGEXP_EXTRACT(supplier_name, r'契約No\.2022000(\d)'), '-1'))

      -- 三井住友トラスト・パナソニックファイナンス パターン
      WHEN STARTS_WITH(supplier_name, '三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始')
           AND REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$') IS NOT NULL
           AND LENGTH(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$')) = 4
      THEN LAST_DAY(
        DATE(
          CAST(CONCAT('20', SUBSTR(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$'), 1, 2)) AS INT64),
          CAST(SUBSTR(REGEXP_EXTRACT(supplier_name, r'_契約開始(\d{4})$'), 3, 2) AS INT64),
          1
        )
      )

      -- デフォルト: fixed_asset_registerから取得した値を使用
      ELSE first_shipped_at_base
    END AS first_shipped_at

  FROM base_stock_data
),

-- レベル2: 償却α/β/γの計算
depreciation_periods AS (
  SELECT
    *,

    -- 償却α（期首時点償却済み月数）
    CASE
      WHEN first_shipped_at >= period_start OR first_shipped_at IS NULL THEN 0

      -- 減損が期首前に発生
      WHEN impairment_date < period_start
           AND ((impossibled_at IS NULL AND lease_start_at IS NULL)
                OR impossibled_at > impairment_date
                OR lease_start_at > impairment_date)
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impairment_date) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impairment_date) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 貸手リースが期首前に開始
      WHEN lease_start_at < period_start
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM lease_start_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM lease_start_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- 除却が期首前（庫内紛失／棚卸差異）
      WHEN impossibled_at < period_start
           AND classification_of_impossibility = '庫内紛失／棚卸差異'
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 除却が期首前（通常）
      WHEN impossibled_at < period_start
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- デフォルト: 初回出荷日から期首日までの月数
      ELSE
        12 * (EXTRACT(YEAR FROM period_start) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM period_start) - EXTRACT(MONTH FROM first_shipped_at)
    END AS shokyaku_alpha,

    -- 償却β（期末時点償却済み月数）
    CASE
      WHEN first_shipped_at > period_end OR first_shipped_at IS NULL THEN 0

      -- 減損が期末前に発生
      WHEN impairment_date <= period_end
           AND ((impossibled_at IS NULL AND lease_start_at IS NULL)
                OR impossibled_at > impairment_date
                OR lease_start_at > impairment_date)
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impairment_date) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impairment_date) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 貸手リースが期末前に開始
      WHEN lease_start_at <= period_end
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM lease_start_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM lease_start_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- 除却が期末前（庫内紛失／棚卸差異）
      WHEN impossibled_at <= period_end
           AND classification_of_impossibility = '庫内紛失／棚卸差異'
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 除却が期末前（通常）
      WHEN impossibled_at <= period_end
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- デフォルト: 初回出荷日から期末日までの月数+1
      ELSE
        12 * (EXTRACT(YEAR FROM period_end) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM period_end) - EXTRACT(MONTH FROM first_shipped_at) + 1
    END AS shokyaku_beta,

    -- 償却γ（リース再取得時点償却済み月数）
    CASE
      WHEN lease_reacquisition_date IS NULL THEN 0

      -- 減損がリース再取得前に発生
      WHEN impairment_date < DATE_TRUNC(lease_reacquisition_date, MONTH)
           AND ((impossibled_at IS NULL AND lease_start_at IS NULL)
                OR impossibled_at > impairment_date
                OR lease_start_at > impairment_date)
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impairment_date) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impairment_date) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 貸手リースがリース再取得前に開始
      WHEN lease_start_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM lease_start_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM lease_start_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- 除却がリース再取得前（庫内紛失／棚卸差異）
      WHEN impossibled_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
           AND classification_of_impossibility = '庫内紛失／棚卸差異'
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at) + 1,
        0
      )

      -- 除却がリース再取得前（通常）
      WHEN impossibled_at < DATE_TRUNC(lease_reacquisition_date, MONTH)
      THEN GREATEST(
        12 * (EXTRACT(YEAR FROM impossibled_at) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM impossibled_at) - EXTRACT(MONTH FROM first_shipped_at),
        0
      )

      -- デフォルト: 初回出荷日からリース再取得日までの月数
      ELSE
        12 * (EXTRACT(YEAR FROM lease_reacquisition_date) - EXTRACT(YEAR FROM first_shipped_at))
        + EXTRACT(MONTH FROM lease_reacquisition_date) - EXTRACT(MONTH FROM first_shipped_at)
    END AS shokyaku_gamma

  FROM enriched_data
),

-- レベル3〜8: 以降の計算ロジック（取得原価、償却月数、減価償却累計額、減損損失累計額、簿価）
-- ... (詳細は実装時に展開)

SELECT * FROM final_book_values;
```

---

## 📝 実装ステップ

### Phase 1: 基礎データ層（レベル0〜1）

#### ステップ1-1: base_stock_data CTEの実装
- [ ] 4テーブルのJOIN実装
- [ ] 実際の取得原価の計算
- [ ] 月次償却額の計算
- [ ] stock_id=3で動作確認

**検証クエリ**:
```sql
DECLARE period_start DATE DEFAULT DATE '2025-03-01';
DECLARE period_end DATE DEFAULT DATE '2025-11-30';

WITH base_stock_data AS (
  -- 実装コード
)
SELECT * FROM base_stock_data WHERE stock_id = 3;
```

**期待結果**:
- stock_id: 3
- actual_cost: 10281
- monthly_depreciation: 171.35 (10281 / 60)
- depreciation_period: 5

---

#### ステップ1-2: enriched_data CTEの実装
- [ ] 契約識別コードの抽出ロジック
- [ ] リース再取得日の計算ロジック（2パターン）
- [ ] 初回出荷日の計算ロジック（2パターン）
- [ ] stock_id=3で動作確認

**検証クエリ**:
```sql
WITH ... enriched_data AS (...)
SELECT
  stock_id,
  supplier_name,
  contract_code,
  lease_reacquisition_date,
  first_shipped_at
FROM enriched_data
WHERE stock_id = 3;
```

---

### Phase 2: 償却期間層（レベル2）

#### ステップ2-1: depreciation_periods CTEの実装
- [ ] 償却αの実装（Looker Studioロジック完全移植）
- [ ] 償却βの実装
- [ ] 償却γの実装
- [ ] stock_id=3で動作確認

**検証クエリ**:
```sql
WITH ... depreciation_periods AS (...)
SELECT
  stock_id,
  first_shipped_at,
  shokyaku_alpha,
  shokyaku_beta,
  shokyaku_gamma
FROM depreciation_periods
WHERE stock_id = 3;
```

**期待結果**:
- first_shipped_at: 2018-10-14
- shokyaku_alpha: 77ヶ月 (2018-10 〜 2025-03)
- shokyaku_beta: 86ヶ月 (2018-10 〜 2025-11 + 1)
- shokyaku_gamma: 0 (リース再取得日なし)

---

### Phase 3: 取得原価層（レベル3）

#### ステップ3-1: acquisition_costs CTEの実装
- [ ] レベシェア判定の共通ロジック作成
- [ ] 期首取得原価の実装（リース再取得日判定含む）
- [ ] 増加取得原価の実装
- [ ] 減少取得原価の実装
- [ ] 期末取得原価の実装（リース再取得日判定含む）
- [ ] stock_id=3で動作確認

**レベシェア判定ロジック**:
```sql
(REGEXP_CONTAINS(supplier_name, 'レベシェア')
 AND NOT REGEXP_CONTAINS(supplier_name, 'リース・レベシェア'))
OR REGEXP_CONTAINS(supplier_name, '法人小物管理用')
OR sample
```

---

### Phase 4: 償却月数層（レベル4）

#### ステップ4-1: amortization_months CTEの実装
- [ ] 期首償却月数の実装
- [ ] **償却償却月数の実装（β-γ vs β-α の条件分岐）** ★重要
- [ ] 増加償却月数の実装
- [ ] 減少償却月数の実装
- [ ] 期末償却月数の実装
- [ ] stock_id=3で動作確認

---

### Phase 5: 減価償却累計額層（レベル5）

#### ステップ5-1: accumulated_depreciation CTEの実装
- [ ] 期首減価償却累計額の実装
- [ ] 増加減価償却累計額の実装
- [ ] 減少減価償却累計額の実装
- [ ] 期中減価償却費の実装（複雑なロジック）
- [ ] 期末減価償却累計額の実装
- [ ] stock_id=3で動作確認

---

### Phase 6: 簿価仮計算層（レベル6）

#### ステップ6-1: book_values_temp CTEの実装
- [ ] 仮期首簿価の計算
- [ ] 仮増加簿価の計算
- [ ] stock_id=3で動作確認

**目的**: 増加減損損失累計額の計算で循環参照を回避

---

### Phase 7: 減損損失累計額層（レベル7）

#### ステップ7-1: impairment_losses CTEの実装
- [ ] 期首減損損失累計額の実装
- [ ] **増加減損損失累計額の実装（修正版ロジック使用）**
- [ ] 減少減損損失累計額の実装
- [ ] 期中減損損失の実装
- [ ] 期末減損損失累計額の実装
- [ ] stock_id=3で動作確認

**修正版ロジック（増加減損損失累計額）**:
```sql
CASE
  WHEN レベシェア判定 THEN 0
  WHEN impairment_date > DATE_TRUNC(lease_reacquisition_date, MONTH) THEN 0
  WHEN (impairment_date > inspected_at AND lease_reacquisition_date IS NULL) THEN 0
  WHEN impairment_date IS NULL THEN 0
  ELSE acquisition_cost_increase - accumulated_depreciation_increase
END
```

---

### Phase 8: 最終簿価層（レベル8）

#### ステップ8-1: final_book_values CTEの実装
- [ ] 期首簿価の実装
- [ ] 増加簿価の実装
- [ ] 減少簿価の実装
- [ ] 期末簿価の実装
- [ ] stock_id=3で動作確認

**簿価計算式**:
```sql
book_value_opening = acquisition_cost_opening
                     - accumulated_depreciation_opening
                     - impairment_loss_opening
```

---

### Phase 9: テーブル作成と検証

#### ステップ9-1: 小規模テスト実行
- [ ] stock_id=3のみでテスト実行
- [ ] Pythonの固定資産台帳と比較検証

**テストクエリ**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.fixed_asset_register_sql_test` AS
-- 全CTEを統合したクエリ
SELECT * FROM final_book_values
WHERE stock_id = 3;
```

---

#### ステップ9-2: 全データでのテーブル作成
- [ ] 20万件のテーブル作成
- [ ] 実行時間の測定
- [ ] エラーの有無を確認

**本番テーブル作成**:
```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.fixed_asset_register_sql_20251130` AS
-- 全CTEを統合したクエリ
SELECT * FROM final_book_values;
```

**テーブル名の命名規則**: `fixed_asset_register_sql_YYYYMMDD`（期末日）

---

#### ステップ9-3: Pythonとの比較検証
- [ ] 10件のランダムなstock_idで比較
- [ ] 簿価の合計値が一致するか確認
- [ ] 差異がある場合、原因を特定

**検証クエリ**:
```sql
-- 期末簿価の合計値
SELECT SUM(book_value_closing) AS total_book_value
FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`;

-- ランダムに10件抽出して詳細比較
SELECT *
FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`
WHERE stock_id IN (
  SELECT stock_id
  FROM `clas-analytics.finance.fixed_asset_register_sql_20251130`
  ORDER BY RAND()
  LIMIT 10
);
```

---

## 🧪 テスト計画

### 単体テスト（各CTEレベルごと）

#### テスト1: base_stock_data
- [ ] stock_id=3で取得原価が10281円
- [ ] 月次償却額が171.35円

#### テスト2: enriched_data
- [ ] 契約識別コードが正しく抽出されるか
- [ ] リース再取得日が正しく計算されるか
- [ ] 初回出荷日が正しく計算されるか

#### テスト3: depreciation_periods
- [ ] stock_id=3で償却α、β、γが正しく計算されるか
- [ ] 減損・貸手リース・除却の優先順位が正しいか

#### テスト4: acquisition_costs
- [ ] 期首取得原価でリース再取得日判定が正しく動作するか
- [ ] 増加取得原価でリース再取得OR新規入庫判定が正しいか

#### テスト5: amortization_months
- [ ] 償却償却月数でβ-γ vs β-α切り替えが正しいか

#### テスト6: accumulated_depreciation
- [ ] 期首減価償却累計額でリース再取得日判定が正しいか
- [ ] 期中減価償却費の複雑なロジックが正しいか

#### テスト7: impairment_losses
- [ ] 増加減損損失累計額で仮簿価を使った計算が正しいか

#### テスト8: final_book_values
- [ ] 期首簿価 + 増加簿価 - 減少簿価 = 期末簿価が成立するか

---

### 統合テスト

#### テスト9: Pythonとの完全一致確認
- [ ] stock_id=3の全項目がPythonと一致するか（許容誤差: ±0.01円）
- [ ] 10件のランダムstock_idで全項目が一致するか

#### テスト10: データ品質チェック
- [ ] 簿価の整合性（期首+増加-減少=期末）が全レコードで成立するか
- [ ] 簿価がマイナスになるレコードがないか（減損除く）
- [ ] 減価償却累計額 ≤ 取得原価が全レコードで成立するか

---

## 📊 出力テーブルのスキーマ

```sql
CREATE OR REPLACE TABLE `clas-analytics.finance.fixed_asset_register_sql_20251130` AS
SELECT
  -- 識別情報
  stock_id INT64,
  part_id INT64,
  part_name STRING,
  supplier_name STRING,

  -- 基礎データ
  actual_cost FLOAT64,  -- 実際の取得原価
  depreciation_period INT64,  -- 耐用年数
  monthly_depreciation FLOAT64,  -- 月次償却額

  -- 日付情報
  inspected_at DATE,  -- 入庫検品完了日
  first_shipped_at DATE,  -- 初回出荷日（計算後）
  lease_reacquisition_date DATE,  -- リース再取得日
  impossibled_at DATE,  -- 除売却日
  impairment_date DATE,  -- 減損損失日
  lease_start_at DATE,  -- 貸手リース開始日

  -- フラグ・分類
  sample BOOLEAN,
  classification_of_impossibility STRING,

  -- 償却期間（α/β/γ）
  shokyaku_alpha INT64,
  shokyaku_beta INT64,
  shokyaku_gamma INT64,

  -- 取得原価（4時点）
  acquisition_cost_opening FLOAT64,
  acquisition_cost_increase FLOAT64,
  acquisition_cost_decrease FLOAT64,
  acquisition_cost_closing FLOAT64,

  -- 償却月数（5時点）
  amortization_months_opening INT64,
  amortization_months_depreciation INT64,
  amortization_months_increase INT64,
  amortization_months_decrease INT64,
  amortization_months_closing INT64,

  -- 減価償却累計額（5時点）
  accumulated_depreciation_opening FLOAT64,
  accumulated_depreciation_increase FLOAT64,
  accumulated_depreciation_decrease FLOAT64,
  interim_depreciation_expense FLOAT64,
  accumulated_depreciation_closing FLOAT64,

  -- 減損損失累計額（5時点）
  impairment_loss_opening FLOAT64,
  impairment_loss_increase FLOAT64,
  impairment_loss_decrease FLOAT64,
  interim_impairment_loss FLOAT64,
  impairment_loss_closing FLOAT64,

  -- 簿価（4時点）★最重要
  book_value_opening FLOAT64,
  book_value_increase FLOAT64,
  book_value_decrease FLOAT64,
  book_value_closing FLOAT64,

  -- 固定値（参照用）
  period_start DATE,  -- 2025-03-01
  period_end DATE     -- 2025-11-30

FROM final_book_values;
```

**カラム数**: 42カラム
**レコード数**: 約20万件

---

## 🎯 成功基準

### 必須条件
1. **Pythonとの完全一致**:
   - stock_id=3の全項目が一致（許容誤差: ±0.01円）
   - ランダム10件の全項目が一致

2. **データ品質**:
   - 簿価の整合性: 期首+増加-減少=期末（誤差0.01円以内）
   - 減価償却累計額 ≤ 取得原価
   - NULL値がない（意図的なNULL除く）

3. **実行時間**:
   - 20万件のテーブル作成が10分以内

---

## 📅 実装スケジュール

| フェーズ | 作業内容 | 所要時間 |
|---------|---------|---------|
| Phase 1 | 基礎データ層（レベル0〜1） | 2時間 |
| Phase 2 | 償却期間層（レベル2） | 2時間 |
| Phase 3 | 取得原価層（レベル3） | 1時間 |
| Phase 4 | 償却月数層（レベル4） | 1時間 |
| Phase 5 | 減価償却累計額層（レベル5） | 2時間 |
| Phase 6 | 簿価仮計算層（レベル6） | 0.5時間 |
| Phase 7 | 減損損失累計額層（レベル7） | 1時間 |
| Phase 8 | 最終簿価層（レベル8） | 0.5時間 |
| Phase 9 | テーブル作成と検証 | 2時間 |
| **合計** | | **12時間** |

**推定実装期間**: 2営業日

---

## 🔄 今後の拡張（Phase 9完了後）

### ステップ10: 複数期間への拡張
- [ ] 計算期間マスタテーブルの作成
- [ ] 期間ごとのCROSS JOINロジック追加
- [ ] 13ヶ月分のテーブル作成
- [ ] スケジュールクエリの設定

**拡張後のテーブル名**: `book_value_calculation`（260万件）

---

## 📝 次のアクション

1. **Phase 1の実装開始**: base_stock_data CTEとenriched_data CTEの実装
2. **stock_id=3での逐次検証**: 各CTEレベルごとにPythonと比較
3. **完全なSQLの作成**: 全CTEを統合した完全なクエリ

実装開始の準備が整いました。Phase 1から開始してよろしいでしょうか?

---

**作成者**: Claude Code
**最終更新日**: 2025-12-26
**バージョン**: 1.0
