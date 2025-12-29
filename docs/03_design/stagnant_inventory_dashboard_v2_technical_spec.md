# 滞留在庫ダッシュボード v2 技術仕様書（詳細版）

**作成日**: 2025-12-24
**ステータス**: 完了
**対象ファイル**: `stagnant_inventory_dashboard_v2.sql`

---

## 目次

1. [概要](#概要)
2. [背景と目的](#背景と目的)
3. [データモデル概要](#データモデル概要)
4. [CTE詳細解説](#cte詳細解説)
5. [重複SKU除外ロジック](#重複sku除外ロジック)
6. [除外条件の定義](#除外条件の定義)
7. [出力カラム一覧](#出力カラム一覧)
8. [注意事項・制約](#注意事項制約)

---

## 概要

滞留在庫ダッシュボードは、SKU単位で滞留在庫の状況を可視化し、各期末における減損予定数と取得原価を算出するためのBigQueryクエリです。

### 主な機能

| 機能 | 説明 |
|---|---|
| 減損予定数の算出 | 期末1〜4以降の各期間で減損対象となる商品数・余りパーツ数を計算 |
| 取得原価の集計 | 減損予定在庫の取得原価（`stock.cost`）を期間別に集計 |
| 組み上げ計算 | 複数パーツで構成されるSKUの組み上げ可能数をエリア別に計算 |
| 在庫ID表示 | 減損対象の在庫IDを`[パーツid:倉庫名]在庫id`形式で表示 |
| 滞留日数計算 | パーツ単位の平均滞留日数を算出 |

---

## 背景と目的

### ビジネス背景

- 在庫は365日以上滞留すると減損対象となる
- 期末（2月末、5月末、8月末、11月末）に減損処理を実施
- 事前に減損予定数と金額を把握し、在庫回転施策を検討する必要がある

### v2での主な変更点

| 項目 | v1 | v2 |
|---|---|---|
| カテゴリ方式 | 累計方式（重複カウント） | 排他カテゴリ方式（1在庫1カテゴリ） |
| 期末日 | 固定値 | 動的計算（実行日から自動算出） |
| エリア計算 | 全エリア合算 | エリア別計算後に合算 |
| 取得原価 | なし | 期間別取得原価カラム追加 |
| 倉庫名表示 | なし | 在庫IDに倉庫名を付与 |

---

## データモデル概要

### 主要テーブル関連図

```
lake.sku
    └── lake.product (product_id)
            └── lake.series (series_id)
            └── lake.attribute → lake.attribute_value → lake.attribute_value_part
                                                            └── lake.part

lake.stock
    └── lake.part (part_id)
    └── lake.location (location_id) → lake.warehouse
    └── lake.lent / lake.lent_to_b (出荷・返却履歴)
```

### SKUとパーツの関係

CLASのSKUは「属性値（attribute_value）の組み合わせ」で定義されます：

```
SKU.hash = SHA1(body_av_id, leg_av_id, mattress_av_id, mattress_topper_av_id, guarantee_av_id)
```

例: ベッドフレームSKU
- Body: フレーム本体 → パーツ8049（quantity: 3）
- Leg: 脚部品 → パーツ8059（quantity: 1）
→ このSKUは「パーツ8049×3点 + パーツ8059×1点」で1商品となる

---

## CTE詳細解説

### 0. SKU連番生成

#### `max_sku_id` (行41-43)
**目的**: SKUテーブルの最大IDを取得

```sql
max_sku_id AS (
  SELECT MAX(id) AS max_id FROM `lake.sku`
)
```

**使用意図**: 削除済みSKUも含めて1〜MAX(id)の全連番を出力するため。欠番のSKU_IDも行として出力される。

#### `all_sku_ids` (行44-47)
**目的**: 1からMAX(SKU_ID)までの連番を生成

```sql
all_sku_ids AS (
  SELECT seq_id
  FROM max_sku_id, UNNEST(GENERATE_ARRAY(1, max_id)) AS seq_id
)
```

**使用意図**:
- 最終出力でこのテーブルを起点にLEFT JOINすることで、欠番SKU_IDも含めた全連番を出力
- 削除済みSKUは「除外」フラグ付きで出力される

---

### 0-1. 属性タイプ別の共通CTE

#### `attribute_value_by_type` (行52-60)
**目的**: 商品→属性→属性値の関連を取得（クエリ内5箇所で再利用）

```sql
attribute_value_by_type AS (
  SELECT
    a.product_id,
    a.type,           -- Body, Leg, Mattress, MattressTopper, Guarantee
    av.id AS av_id
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
)
```

**使用意図**:
- `type`カラムでBody, Leg等のパーツ種別を識別
- 削除済みレコードは除外（論理削除対応）
- このCTEを共通化することでクエリの可読性向上と重複排除

**利用箇所**:
1. `sku_part_mapping` - SKUとパーツの紐付け
2. `sku_part_detail` - パーツ詳細情報取得
3. `current_attribute_hashes` - 有効なSKUハッシュ判定
4. `dup_sku_attribute_mapping` - 重複SKU判定
5. `attribute_based_info` - 属性ベースの情報取得

---

### 1. 動的期末日計算

#### `quarter_ends` (行66-123)
**目的**: 実行日から次の4つの期末日（2月末/5月末/8月末/11月末）を動的に算出

```sql
quarter_ends AS (
  SELECT
    CURRENT_DATE() AS today,
    -- 今年と来年の期末候補から、今日より後の日付を昇順で取得
    (SELECT MIN(candidate_date)
     FROM UNNEST([
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 28),
       DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 5, 31),
       -- ... 今年と来年の8候補
     ]) AS candidate_date
     WHERE candidate_date > CURRENT_DATE()
    ) AS term1_end,
    -- term2_end, term3_end, term4_end も同様にOFFSET(1), (2), (3)で取得
)
```

**使用意図**:
- v1では固定値だった期末日を動的計算に変更
- 実行日に依存せず常に「次の期末」から順に4つを取得
- 例: 12月24日実行 → term1=2月28日, term2=5月31日, term3=8月31日, term4=11月30日

**期末月の選定理由**:
CLASの会計期間に合わせた期末月（2月末/5月末/8月末/11月末）

---

### 2. 引当可能在庫の抽出

#### `stocks` (行128-203)
**目的**: 減損判定の対象となる「引当可能な在庫」を抽出

```sql
stocks AS (
  SELECT
    s.id,
    s.status,
    s.part_id,
    w.business_area,           -- Kanto, Kansai, Kyushu
    CASE w.name                 -- 倉庫名の短縮表記
      WHEN '法人直送' THEN '船橋'
      WHEN '門真倉庫' THEN '門真'
      -- ...
    END AS warehouse_name,
    s.impairment_date,
    s.cost                      -- 取得原価（v2で追加）
  FROM `lake.stock` s
  INNER JOIN `lake.part` p ON p.id = s.part_id AND p.deleted_at IS NULL
  INNER JOIN `lake.part_version` pv ON pv.id = s.part_version_id
  INNER JOIN `lake.location` l ON l.id = s.location_id
  INNER JOIN `lake.warehouse` w ON w.id = l.warehouse_id
  -- 除外用サブクエリ群（LEFT OUTER JOIN）
  LEFT OUTER JOIN (...) to_c   -- C向け引当中
  LEFT OUTER JOIN (...) to_b   -- B向け引当中
  LEFT OUTER JOIN (...) tag    -- 特定タグ付き
  LEFT OUTER JOIN (...) external_sale  -- 外部販売対象
  WHERE
    s.deleted_at IS NULL
    AND (s.status IN ('Ready', 'Waiting')
         OR (s.status = 'Recovery' AND s.part_id NOT IN (特定パーツID)))
    AND tag.stock_id IS NULL         -- タグ付きでない
    AND to_c.stock_id IS NULL        -- C向け引当中でない
    AND to_b.stock_id IS NULL        -- B向け引当中でない
    AND w.available_for_business = TRUE
    AND s._rank_ NOT IN ('R', 'L')   -- ランクR/Lは除外
    AND l.id NOT IN (特定ロケーションID)
    AND external_sale.stock_id IS NULL
)
```

**除外条件の詳細**:

| 条件 | 理由 |
|---|---|
| `s.deleted_at IS NULL` | 論理削除された在庫を除外 |
| `status IN ('Ready', 'Waiting')` | 引当可能なステータスのみ |
| `status = 'Recovery' AND part_id NOT IN (...)` | Recovery状態でも特定パーツは引当可能 |
| `to_c.stock_id IS NULL` | C向け（個人顧客）引当準備中を除外 |
| `to_b.stock_id IS NULL` | B向け（法人）引当準備中を除外 |
| `tag.stock_id IS NULL` | 特定タグ(mt.id=64)付き在庫を除外 |
| `w.available_for_business = TRUE` | 営業可能な倉庫のみ |
| `s._rank_ NOT IN ('R', 'L')` | ランクR（返却不可）/L（廃棄）を除外 |
| `l.id NOT IN (...)` | 特定ロケーション（検品中等）を除外 |
| `external_sale.stock_id IS NULL` | 外部販売予定在庫を除外 |

**倉庫名の短縮マッピング**:

| 元の名称 | 短縮名 |
|---|---|
| 法人直送 | 船橋 |
| 門真倉庫 | 門真 |
| 東葛西ロジスティクスセンター | 東葛西 |
| 株式会社MAKE VALUE　川崎倉庫 | 川崎 |
| キタザワ九州倉庫 | 九州 |

---

### 3. 属性値単位の在庫集計

#### `avs` (行208-268)
**目的**: 属性値（AttributeValue）単位でエリア別在庫数を集計

```sql
avs AS (
  SELECT
    ANY_VALUE(a.product_id) AS product_id,
    ANY_VALUE(a.`type`) AS type,
    CONCAT(...) AS value,  -- 「本体: ナチュラル」のような表示用値
    av.id,
    IFNULL(MIN(avp.cnt_kanto), 0) AS cnt_kanto,
    IFNULL(MIN(avp.cnt_kansai), 0) AS cnt_kansai,
    IFNULL(MIN(avp.cnt_kyushu), 0) AS cnt_kyushu,
    -- Recovery含む在庫数も別途集計
    ARRAY_AGG(DISTINCT supplier) AS suppliers,
    IFNULL(SUM(avp.cost), 0) AS cost
  FROM `lake.attribute` a
  INNER JOIN `lake.attribute_value` av ON a.id = av.attribute_id
  LEFT OUTER JOIN (
    -- 各属性値パーツの在庫数をエリア別にカウント
    SELECT
      avp.attribute_value_id,
      CAST((IFNULL(available_stock_kanto.cnt, 0)) / avp.quantity AS INT64) AS cnt_kanto,
      -- ...
    FROM `lake.attribute_value_part` avp
    -- 各エリアの在庫カウントをJOIN
  ) avp ON avp.av_id = av.id
  GROUP BY av.id
)
```

**使用意図**:
- 属性値（例: "本体: ナチュラル"）単位での在庫数把握
- quantityで割ることで「何セット分の在庫があるか」を計算
- `MIN`を使う理由: 複数パーツで構成される場合、最も少ないパーツ数が組み上げ可能数の上限

---

### 4. カテゴリ名の日本語変換

#### `category_mapping` (行273-326)
**目的**: 英語カテゴリ名を日本語に変換するマッピングテーブル

```sql
category_mapping AS (
  SELECT category_en, category_ja FROM UNNEST([
    STRUCT('Sofa' AS category_en, 'ソファ' AS category_ja),
    STRUCT('Bed', 'ベッド・寝具'),
    STRUCT('Chair', 'チェア'),
    -- ... 40種類以上のカテゴリ
  ])
)
```

**使用意図**:
- `lake.series.category`は英語で格納されているため、ダッシュボード表示用に日本語変換
- UNNEST+STRUCT方式で外部テーブル不要のマッピングを実現

---

### 5. 滞留日数計算

#### `stock_arrivals` (行332-341)
**目的**: 各在庫の入庫日を特定（資産台帳を優先）

```sql
stock_arrivals AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    COALESCE(MAX(fa.arrival_at), ANY_VALUE(s.arrival_at)) AS arrival_at
  FROM `lake.stock` s
  LEFT JOIN `finance.fixed_asset_register` fa ON s.id = fa.stock_id
  WHERE s.deleted_at IS NULL
  GROUP BY s.id, s.part_id
)
```

**使用意図**:
- `stock.arrival_at`が`9999-12-31`の場合がある（未入庫扱い）
- その場合は資産台帳（fixed_asset_register）の入庫日を使用
- `COALESCE`で優先順位付け

#### `first_ec_ship` / `first_b2b_ship` (行344-365)
**目的**: 各在庫の初回出荷日を特定

```sql
first_ec_ship AS (
  SELECT detail.stock_id, MIN(lent.shipped_at) AS first_shipped_at
  FROM `lake.lent` lent
  INNER JOIN `lake.lent_detail` detail ON lent.id = detail.lent_id
  WHERE lent.status NOT IN ('Cancel', 'PurchaseFailed')
    AND lent.shipped_at IS NOT NULL
  GROUP BY detail.stock_id
)
```

**使用意図**:
- 新品（1周目）か中古（2周目以降）かを判定するために使用
- 初回出荷がなければ新品、あれば中古として滞留開始日を決定

#### `all_lent_history` (行368-383)
**目的**: 全出荷・返却履歴を時系列で取得

```sql
all_lent_history AS (
  -- B向け出荷
  SELECT stock_id, start_at AS event_date, 'B2B_Ship' AS event_type
  FROM `lake.lent_to_b` ...
  UNION ALL
  -- C向け返却
  SELECT stock_id, return_at AS event_date, 'EC_Return' AS event_type
  FROM `lake.lent` ... WHERE status = 'Returned'
  UNION ALL
  -- B向け返却
  SELECT stock_id, end_at AS event_date, 'B2B_Return' AS event_type
  FROM `lake.lent_to_b` ... WHERE status = 'Ended'
)
```

**使用意図**:
- 2周目以降の在庫の滞留開始日 = 最終返却日
- 出荷・返却履歴を統合して最新のイベントを特定

#### `retention_round_1_waiting` / `retention_round_2_waiting` (行386-414)
**目的**: 滞留日数を1周目と2周目以降で分けて計算

```sql
-- 1周目: 新品入庫 → 現在（一度も出荷されていない）
retention_round_1_waiting AS (
  SELECT
    sa.part_id,
    sa.stock_id,
    DATE_DIFF(CURRENT_DATE(), CAST(sa.arrival_at AS DATE), DAY) AS duration_days
  FROM stock_arrivals sa
  INNER JOIN stocks st ON sa.stock_id = st.id
  LEFT JOIN first_b2b_ship b2b ON sa.stock_id = b2b.stock_id
  LEFT JOIN first_ec_ship ec ON sa.stock_id = ec.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND b2b.first_start_at IS NULL    -- B向け出荷履歴なし
    AND ec.first_shipped_at IS NULL   -- C向け出荷履歴なし
)

-- 2周目以降: 最終返却 → 現在
retention_round_2_waiting AS (
  SELECT
    t.stock_id,
    DATE_DIFF(CURRENT_DATE(), t.event_date, DAY) AS duration_days
  FROM (
    SELECT stock_id, event_date, event_type,
           ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY event_date DESC) as rn
    FROM all_lent_history
  ) t
  WHERE t.rn = 1
    AND t.event_type IN ('EC_Return', 'B2B_Return')  -- 最新が返却イベント
)
```

**使用意図**:
- 1周目: 入庫日から現在までの日数
- 2周目以降: 最終返却日から現在までの日数
- `ROW_NUMBER() OVER (... ORDER BY event_date DESC)`で最新のイベントを特定

#### `sku_current_retention_avg` (行453-460)
**目的**: SKUごとの平均滞留日数を算出

```sql
sku_current_retention_avg AS (
  SELECT
    spm.sku_hash,
    ROUND(AVG(pcra.avg_current_retention_days), 1) AS avg_current_retention_days
  FROM sku_part_mapping spm
  LEFT JOIN part_current_retention_avg pcra ON spm.part_id = pcra.part_id
  GROUP BY spm.sku_hash
)
```

**使用意図**:
- SKUを構成するパーツの平均滞留日数をSKU単位で集約
- ダッシュボード表示用（`滞留日数_平均_切上`カラム）

---

### 6. 減損計算（コア処理）

#### `stock_retention_start` (行465-503)
**目的**: 各在庫の滞留開始日を特定（減損判定の基準日）

```sql
stock_retention_start AS (
  -- 1周目: 新品入庫→現在在庫中
  SELECT
    sa.stock_id, s.part_id, s.business_area, s.warehouse_name,
    s.impairment_date, s.cost,
    CAST(sa.arrival_at AS DATE) AS retention_start_date
  FROM stock_arrivals sa
  INNER JOIN stocks s ON sa.stock_id = s.id
  LEFT JOIN first_b2b_ship b2b ON sa.stock_id = b2b.stock_id
  LEFT JOIN first_ec_ship ec ON sa.stock_id = ec.stock_id
  WHERE sa.arrival_at IS NOT NULL
    AND b2b.first_start_at IS NULL
    AND ec.first_shipped_at IS NULL

  UNION ALL

  -- 2周目以降: 最終返却日→現在在庫中
  SELECT
    t.stock_id, s.part_id, s.business_area, s.warehouse_name,
    s.impairment_date, s.cost,
    CAST(t.event_date AS DATE) AS retention_start_date
  FROM (
    SELECT stock_id, event_date, event_type,
           ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY event_date DESC) as rn
    FROM all_lent_history
  ) t
  INNER JOIN stocks s ON t.stock_id = s.id
  WHERE t.rn = 1
    AND t.event_type IN ('EC_Return', 'B2B_Return')
)
```

**使用意図**:
- **滞留開始日の決定ルール**:
  - 新品（一度も出荷されていない）: 入庫日
  - 2周目以降（返却後在庫中）: 最終返却日（EC返却 or B2B返却の最新日）
- `cost`カラムを伝播させて後続の取得原価計算で使用

#### `sku_part_detail` (行508-531)
**目的**: SKUごとのパーツ構成とquantity情報を取得

```sql
sku_part_detail AS (
  SELECT
    sku.id AS sku_id,
    sku.hash AS sku_hash,
    avp.part_id,
    p.name AS part_name,
    a.type AS part_type,     -- Body, Leg, Mattress, etc.
    avp.quantity             -- そのパーツが何点必要か
  FROM `lake.sku` sku
  INNER JOIN `lake.product` pd ON pd.id = sku.product_id
  -- 属性タイプ別にLEFT JOIN
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg ON leg.product_id = pd.id AND leg.type = 'Leg'
  LEFT JOIN attribute_value_by_type mr ON mr.product_id = pd.id AND mr.type = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt ON mrt.product_id = pd.id AND mrt.type = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr ON gr.product_id = pd.id AND gr.type = 'Guarantee'
  -- 全属性値IDをUNNESTして展開
  CROSS JOIN UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id
  INNER JOIN `lake.part` p ON p.id = avp.part_id
  WHERE sku.deleted_at IS NULL
    -- SKUハッシュが正しく計算されているか検証
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(
      ARRAY(SELECT CAST(element AS STRING)
            FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id])
            AS element ORDER BY element), ',')))
    AND av_id IS NOT NULL
)
```

**使用意図**:
- SKUが「どのパーツを何点で構成されているか」を明確化
- 例: SKU ID 45 → パーツ8049×3点 + パーツ8059×1点
- Guaranteeはパーツがないため、av_idのUNNEST対象から除外

**SKUハッシュの検証**:
`sku.hash = TO_HEX(SHA1(...))` の条件は、SKUテーブルに格納されているハッシュ値と、実際の属性値IDから計算したハッシュ値が一致することを確認。これにより、不整合なSKUレコードを除外。

#### `stock_with_category` (行551-579)
**目的**: 各在庫に排他的カテゴリを付与（減損判定の核心）

```sql
stock_with_category AS (
  SELECT
    srs.stock_id, srs.part_id, srs.business_area, srs.warehouse_name,
    srs.retention_start_date, srs.cost,
    -- 排他的カテゴリ判定（動的期末日を使用）
    CASE
      -- 既に減損処理済み
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 'impaired'
      -- 滞留開始日+365日が現在日を超過 → 減損済み扱い
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 'impaired'
      -- 期末1までに減損
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 'term1'
      -- 期末2までに減損
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 'term2'
      -- 期末3までに減損
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 'term3'
      -- 期末4以降
      ELSE 'term4_after'
    END AS category,
    -- 優先順位（商品判定用、減損済みは99）
    CASE
      WHEN srs.impairment_date IS NOT NULL AND srs.impairment_date <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= CURRENT_DATE() THEN 99
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term1_end THEN 1
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term2_end THEN 2
      WHEN DATE_ADD(srs.retention_start_date, INTERVAL 365 DAY) <= qe.term3_end THEN 3
      ELSE 4
    END AS category_order
  FROM stock_retention_start srs
  CROSS JOIN quarter_ends qe
)
```

**使用意図**:
- **排他カテゴリ方式**: 各在庫は1つのカテゴリにのみ属する（v1の累計方式との違い）
- カテゴリ判定ロジック:
  1. `impairment_date`が設定済み → 減損済み
  2. 滞留開始日+365日 ≤ 現在日 → 減損済み
  3. 滞留開始日+365日 ≤ 期末1 → term1
  4. 滞留開始日+365日 ≤ 期末2 → term2
  5. 滞留開始日+365日 ≤ 期末3 → term3
  6. それ以外 → term4_after
- `category_order`は商品判定時に「最も早い減損カテゴリ」を取得するために使用

#### `all_areas` (行584-586)
**目的**: 在庫が存在するエリアのリストを取得

```sql
all_areas AS (
  SELECT DISTINCT business_area FROM stock_with_category
)
```

**使用意図**:
- 後続のCROSS JOINでSKU×エリア×パーツの全組み合わせを生成するため
- 在庫が存在しないエリアは計算対象外

#### `sku_area_part_full` (行591-595)
**目的**: SKU×エリア×パーツの全組み合わせを生成

```sql
sku_area_part_full AS (
  SELECT spd.sku_hash, spd.part_id, spd.part_type, spd.quantity, areas.business_area
  FROM sku_part_detail spd
  CROSS JOIN all_areas areas
)
```

**使用意図**:
- **CROSS JOIN方式の重要性**:
  - 片方のパーツがあるエリアに0個でも、組み上げ可能数を正しく0と計算するため
  - 例: SKU ID 45（パーツ8049×3 + パーツ8059×1）
    - 関東: パーツ8049=6点、パーツ8059=0点 → 組み上げ可能数=MIN(6÷3, 0÷1)=**0**
    - 関西: パーツ8049=3点、パーツ8059=2点 → 組み上げ可能数=MIN(3÷3, 2÷1)=**1**

#### `sku_area_part_stock` (行602-619)
**目的**: SKU・エリア・パーツ別の在庫数をカテゴリ別にカウント

```sql
sku_area_part_stock AS (
  SELECT
    sapf.sku_hash, sapf.part_id, sapf.part_type, sapf.quantity, sapf.business_area,
    COUNT(swc.stock_id) AS stock_cnt,
    -- 各カテゴリの在庫数
    COUNT(CASE WHEN swc.category = 'impaired' THEN swc.stock_id END) AS impaired_cnt,
    COUNT(CASE WHEN swc.category = 'term1' THEN swc.stock_id END) AS term1_cnt,
    COUNT(CASE WHEN swc.category = 'term2' THEN swc.stock_id END) AS term2_cnt,
    COUNT(CASE WHEN swc.category = 'term3' THEN swc.stock_id END) AS term3_cnt,
    COUNT(CASE WHEN swc.category = 'term4_after' THEN swc.stock_id END) AS term4_cnt
  FROM sku_area_part_full sapf
  LEFT JOIN stock_with_category swc
    ON sapf.part_id = swc.part_id AND sapf.business_area = swc.business_area
  GROUP BY sapf.sku_hash, sapf.part_id, sapf.part_type, sapf.quantity, sapf.business_area
)
```

**使用意図**:
- LEFT JOINで在庫が0個の場合もCOUNT=0として記録
- カテゴリ別在庫数は後続の分析で使用

#### `sku_area_assemblable` (行624-632)
**目的**: SKU・エリア別の組み上げ可能数を計算

```sql
sku_area_assemblable AS (
  SELECT
    sku_hash,
    business_area,
    -- 組み上げ可能数 = 各パーツの在庫数÷quantityの最小値
    MIN(CAST(stock_cnt / quantity AS INT64)) AS assemblable_cnt
  FROM sku_area_part_stock
  GROUP BY sku_hash, business_area
)
```

**使用意図**:
- **組み上げ可能数の計算式**: `MIN(各パーツの在庫数 ÷ 必要数量)`
- 例: パーツ8049が6点（必要数3）、パーツ8059が2点（必要数1）
  → MIN(6÷3, 2÷1) = MIN(2, 2) = 2商品組み上げ可能
- 1つでも0のパーツがあれば組み上げ不可（=0）

#### `stock_with_order` (行638-663)
**目的**: 組み上げ優先順で在庫に順番を付与

```sql
stock_with_order AS (
  SELECT
    spd.sku_hash, swc.stock_id, swc.part_id, spd.quantity,
    swc.business_area, swc.warehouse_name, swc.category, swc.category_order, swc.cost,
    ROW_NUMBER() OVER (
      PARTITION BY spd.sku_hash, swc.part_id, swc.business_area
      ORDER BY
        CASE swc.category
          WHEN 'term1' THEN 1      -- 最も減損が近い在庫を優先
          WHEN 'term2' THEN 2
          WHEN 'term3' THEN 3
          WHEN 'term4_after' THEN 4
          WHEN 'impaired' THEN 5   -- 減損済みは最後
        END,
        swc.stock_id              -- 同カテゴリ内はID順
    ) AS stock_order
  FROM sku_part_detail spd
  INNER JOIN stock_with_category swc ON spd.part_id = swc.part_id
)
```

**使用意図**:
- **組み上げ優先順位**: term1 > term2 > term3 > term4_after > impaired
- 減損が近い在庫から優先的に商品として組み上げる
  → 余りパーツは減損が遠い在庫になる
- `ROW_NUMBER()`で各パーツの在庫に順番を付与

#### `stock_assembly_classification` (行668-691)
**目的**: 組み上げ用在庫と余り在庫を分離

```sql
stock_assembly_classification AS (
  SELECT
    swo.sku_hash, swo.stock_id, swo.part_id, swo.quantity,
    swo.business_area, swo.warehouse_name, swo.category, swo.category_order, swo.cost,
    swo.stock_order, saa.assemblable_cnt,
    -- 組み上げ用かどうか
    CASE WHEN swo.stock_order <= saa.assemblable_cnt * swo.quantity
         THEN TRUE ELSE FALSE END AS is_assembled,
    -- 商品番号（組み上げ用のみ）
    CASE WHEN swo.stock_order <= saa.assemblable_cnt * swo.quantity
         THEN CAST(CEIL(swo.stock_order / swo.quantity) AS INT64)
         ELSE NULL
    END AS product_no
  FROM stock_with_order swo
  LEFT JOIN sku_area_assemblable saa
    ON swo.sku_hash = saa.sku_hash AND swo.business_area = saa.business_area
)
```

**使用意図**:
- `is_assembled = TRUE`: 商品として組み上げられる在庫
- `is_assembled = FALSE`: 余りパーツ
- `product_no`: 何番目の商品に使われるか（例: quantity=3なら、stock_order 1-3が商品1、4-6が商品2）

**計算例（SKU ID 45: パーツ8049×3 + パーツ8059×1、組み上げ可能数=2）**:

| stock_order | part_id | is_assembled | product_no |
|---|---|---|---|
| 1 | 8049 | TRUE | 1 |
| 2 | 8049 | TRUE | 1 |
| 3 | 8049 | TRUE | 1 |
| 4 | 8049 | TRUE | 2 |
| 5 | 8049 | TRUE | 2 |
| 6 | 8049 | TRUE | 2 |
| 7 | 8049 | FALSE | NULL |
| 1 | 8059 | TRUE | 1 |
| 2 | 8059 | TRUE | 2 |
| 3 | 8059 | FALSE | NULL |

#### `product_category` / `product_with_category` (行696-722)
**目的**: 商品ごとの減損カテゴリを判定

```sql
product_category AS (
  SELECT
    sku_hash, business_area, product_no,
    -- 減損済みを除いて最も早い減損カテゴリを取得
    MIN(CASE WHEN category != 'impaired' THEN category_order END) AS earliest_order
  FROM stock_assembly_classification
  WHERE is_assembled = TRUE AND product_no IS NOT NULL
  GROUP BY sku_hash, business_area, product_no
),

product_with_category AS (
  SELECT
    pc.sku_hash, pc.business_area, pc.product_no,
    CASE
      WHEN pc.earliest_order IS NULL THEN 'all_impaired'  -- 全パーツ減損済み
      WHEN pc.earliest_order = 1 THEN 'term1'
      WHEN pc.earliest_order = 2 THEN 'term2'
      WHEN pc.earliest_order = 3 THEN 'term3'
      WHEN pc.earliest_order = 4 THEN 'term4_after'
    END AS product_category
  FROM product_category pc
)
```

**使用意図**:
- **商品の減損カテゴリ = 構成パーツの中で最も早い減損カテゴリ**
- 例: 商品1を構成するパーツのカテゴリが [term2, term3, impaired] の場合
  → 商品1の減損カテゴリ = term2（impairedを除いた最小）
- 全パーツがimpairedの場合は`all_impaired`

---

### 6-10〜6-12. 集計CTE群

#### `sku_impairment_summary` (行727-744)
**目的**: SKU別の商品数をカテゴリ別に集計

```sql
sku_impairment_summary AS (
  SELECT
    sku_hash,
    COUNT(*) AS total_assemblable,
    SUM(CASE WHEN product_category = 'all_impaired' THEN 1 ELSE 0 END) AS impaired_product_cnt,
    SUM(CASE WHEN product_category = 'term1' THEN 1 ELSE 0 END) AS term1_product_cnt,
    SUM(CASE WHEN product_category = 'term2' THEN 1 ELSE 0 END) AS term2_product_cnt,
    SUM(CASE WHEN product_category = 'term3' THEN 1 ELSE 0 END) AS term3_product_cnt,
    SUM(CASE WHEN product_category = 'term4_after' THEN 1 ELSE 0 END) AS term4_product_cnt
  FROM product_with_category
  GROUP BY sku_hash
)
```

#### `sku_leftover_summary` (行764-781)
**目的**: SKU別の余りパーツ数をカテゴリ別に集計

```sql
sku_leftover_summary AS (
  SELECT
    sku_hash,
    SUM(leftover_cnt) AS total_leftover,
    SUM(CASE WHEN category = 'impaired' THEN leftover_cnt ELSE 0 END) AS leftover_impaired_total,
    SUM(CASE WHEN category = 'term1' THEN leftover_cnt ELSE 0 END) AS leftover_term1_total,
    -- ... term2, term3, term4_after
  FROM sku_area_leftover
  GROUP BY sku_hash
)
```

#### `sku_cost_summary` (行788-809)
**目的**: SKU別の取得原価をカテゴリ別に集計

```sql
sku_cost_summary AS (
  SELECT
    sac.sku_hash,
    -- 期末1減損予定の取得原価（商品＋余りパーツ）
    SUM(CASE WHEN sac.is_assembled AND pwc.product_category = 'term1'
             THEN IFNULL(sac.cost, 0) ELSE 0 END)
      + SUM(CASE WHEN NOT sac.is_assembled AND sac.category = 'term1'
                 THEN IFNULL(sac.cost, 0) ELSE 0 END) AS term1_cost,
    -- term2_cost, term3_cost, term4_cost も同様
  FROM stock_assembly_classification sac
  LEFT JOIN product_with_category pwc
    ON sac.sku_hash = pwc.sku_hash
    AND sac.business_area = pwc.business_area
    AND sac.product_no = pwc.product_no
  GROUP BY sac.sku_hash
)
```

**使用意図**:
- **取得原価の計算方法**:
  - 商品分: `product_category`（商品の減損カテゴリ）で分類
  - 余りパーツ分: `category`（在庫の減損カテゴリ）で分類
- `IFNULL(cost, 0)`: costがNULLの場合は0として計算
- **減損済み（impaired）は取得原価計算から除外**

---

### 6-13. 在庫ID表示用CTE

#### `sku_stock_ids_by_warehouse` (行815-838)
**目的**: パーツ×倉庫別に在庫IDを集約

```sql
sku_stock_ids_by_warehouse AS (
  SELECT
    sac.sku_hash, sac.part_id, sac.warehouse_name,
    -- 商品用在庫ID（商品カテゴリで分類）
    STRING_AGG(CASE WHEN sac.is_assembled AND pwc.product_category = 'term1'
                    THEN CAST(sac.stock_id AS STRING) END, ', ' ORDER BY sac.stock_id) AS term1_assembled_ids,
    -- ... term2, term3, term4, impaired
    -- 余りパーツ用在庫ID（在庫のカテゴリで分類）
    STRING_AGG(CASE WHEN NOT sac.is_assembled AND sac.category = 'term1'
                    THEN CAST(sac.stock_id AS STRING) END, ', ' ORDER BY sac.stock_id) AS term1_leftover_ids,
    -- ... term2, term3, term4, impaired
  FROM stock_assembly_classification sac
  LEFT JOIN product_with_category pwc ON ...
  GROUP BY sac.sku_hash, sac.part_id, sac.warehouse_name
)
```

#### `sku_stock_ids_unified` (行841-858)
**目的**: [パーツid:倉庫名]形式でSKU別に在庫IDを統合

```sql
sku_stock_ids_unified AS (
  SELECT
    sku_hash,
    -- [パーツid:倉庫名]在庫id 形式で集約
    STRING_AGG(
      CASE WHEN term1_assembled_ids IS NOT NULL
           THEN CONCAT('[', CAST(part_id AS STRING), ':', warehouse_name, ']', term1_assembled_ids)
      END, '\n' ORDER BY part_id, warehouse_name
    ) AS term1_assembled_ids,
    -- ... 他のカテゴリも同様
  FROM sku_stock_ids_by_warehouse
  GROUP BY sku_hash
)
```

**出力例**:
```
[8049:船橋]123456, 123457, 123458
[8049:門真]123459, 123460
[8059:船橋]123461, 123462
```

---

## 重複SKU除外ロジック

### 概要

同じパーツ構成を持つ複数のSKUが存在する場合、重複カウントを防ぐために1つを残して他を除外します。

### CTE詳細

#### `dup_sku_attribute_mapping` (行960-982)
**目的**: SKUと属性値のマッピングを取得

#### `dup_sku_with_parts` (行984-1004)
**目的**: SKUごとのパーツ構成をキー化

```sql
dup_sku_with_parts AS (
  SELECT
    sam.sku_id, sam.sku_hash, sam.product_id, sam.product_name, sam.series_name,
    sam.guarantee_av_id,
    -- パーツIDをソートしてカンマ区切りで結合（重複判定キー）
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT CAST(avp.part_id AS STRING) ORDER BY CAST(avp.part_id AS STRING)),
      ','
    ) AS part_id_key,
    ARRAY_AGG(DISTINCT avp.part_id ORDER BY avp.part_id) AS part_ids
  FROM dup_sku_attribute_mapping sam
  CROSS JOIN UNNEST([sam.body_av_id, sam.leg_av_id, sam.mattress_av_id, sam.mattress_topper_av_id]) AS av_id
  INNER JOIN `lake.attribute_value_part` avp ON avp.attribute_value_id = av_id
  WHERE av_id IS NOT NULL
  GROUP BY sam.sku_id, sam.sku_hash, sam.product_id, sam.product_name, sam.series_name, sam.guarantee_av_id
)
```

**使用意図**:
- `part_id_key`: パーツIDを昇順ソートしてカンマ区切りで結合
- 例: パーツ8049と8059で構成 → part_id_key = "8049,8059"
- このキーが同じSKUは「同じパーツ構成」と判定

#### `dup_sku_with_guarantee` (行1021-1028)
**目的**: 補償（Guarantee）情報を付与

```sql
dup_sku_with_guarantee AS (
  SELECT
    swp.*,
    IFNULL(gi.guarantee_name, '補償なし') AS guarantee_name,
    IFNULL(gi.has_damage_guarantee, FALSE) AS has_damage_guarantee
  FROM dup_sku_with_parts swp
  LEFT JOIN dup_guarantee_info gi ON gi.guarantee_av_id = swp.guarantee_av_id
)
```

**使用意図**:
- 汚損補償あり/なしで優先順位を決定するため

#### `dup_sku_full_info` (行1046-1066)
**目的**: 除外対象判定用のフラグを追加

```sql
dup_sku_full_info AS (
  SELECT
    swg.*,
    IFNULL(si.suppliers, '') AS suppliers,
    -- 法人小物管理用フラグ
    CASE WHEN IFNULL(si.suppliers, '') LIKE '%法人小物管理用%' THEN TRUE ELSE FALSE END AS is_corporate_item,
    -- セット商品フラグ（除外対象の名称パターンあり）
    CASE
      WHEN swg.product_name LIKE '%セット%'
        AND swg.product_name NOT LIKE '%増連セット%'
        AND swg.product_name NOT LIKE '%片面開閉カバーセット%'
        AND swg.product_name NOT LIKE '%ハコ4色セット%'
        AND swg.product_name NOT LIKE '%ジョイントセット%'
        AND swg.product_name NOT LIKE '%インセットパネル%'
      THEN TRUE
      ELSE FALSE
    END AS is_set_item
  FROM dup_sku_with_guarantee swg
  LEFT JOIN dup_supplier_info si ON si.sku_id = swg.sku_id
)
```

#### `dup_duplicate_groups` (行1068-1081)
**目的**: 重複グループを特定

```sql
dup_duplicate_groups AS (
  SELECT part_id_key, COUNT(*) AS sku_count
  FROM dup_sku_full_info
  WHERE is_corporate_item = FALSE
    AND is_set_item = FALSE
    AND series_name NOT LIKE '%【商品おまかせでおトク】%'
    AND series_name NOT LIKE '%【CLAS SET】%'
    AND series_name NOT LIKE '%エイトレント%'
    AND product_name NOT LIKE '%プラン%'
  GROUP BY part_id_key
  HAVING COUNT(*) > 1  -- 2つ以上あれば重複
)
```

#### `dup_ranked_duplicates` (行1083-1101)
**目的**: 重複グループ内でランク付け

```sql
dup_ranked_duplicates AS (
  SELECT
    sfi.sku_id,
    ROW_NUMBER() OVER (
      PARTITION BY sfi.part_id_key
      ORDER BY
        sfi.has_damage_guarantee ASC,  -- 汚損補償なしを優先
        sfi.sku_id ASC                  -- SKU_IDが小さい方を優先
    ) AS rank_in_group
  FROM dup_duplicate_groups dg
  INNER JOIN dup_sku_full_info sfi ON dg.part_id_key = sfi.part_id_key
  WHERE ...除外条件...
)
```

**ランク付けルール**:
1. **汚損補償なし**を優先（has_damage_guarantee = FALSE）
2. 同条件なら**SKU_IDが小さい方**を優先

#### `dup_excluded_skus` (行1103-1105)
**目的**: 除外対象のSKU_IDリスト

```sql
dup_excluded_skus AS (
  SELECT sku_id FROM dup_ranked_duplicates WHERE rank_in_group > 1
)
```

**使用意図**:
- rank = 1 のSKUのみ残し、rank > 1 は除外
- 最終SELECTでLEFT JOINし、除外フラグを設定

---

## 除外条件の定義

### 最終出力の除外フラグ判定（行1132-1152）

```sql
CASE
  -- SKUが存在しない（欠番）
  WHEN spi.sku_id IS NULL THEN '除外'
  -- 論理削除されたSKU
  WHEN spi.is_deleted_sku = TRUE THEN '除外'
  -- 孤立SKU（現在の属性構成に存在しない）
  WHEN spi.is_orphan_sku = TRUE THEN '除外'
  -- 法人小物管理用は除外フラグなし（NULL）
  WHEN IFNULL(abi.suppliers, '') LIKE '%法人小物管理用%' THEN NULL
  -- 特定シリーズ名は除外
  WHEN spi.series_name LIKE '%【商品おまかせでおトク】%' THEN '除外'
  WHEN spi.series_name LIKE '%【CLAS SET】%' THEN '除外'
  WHEN spi.series_name LIKE '%エイトレント%' THEN '除外'
  -- セット商品は除外（一部例外あり）
  WHEN spi.product_name LIKE '%セット%'
    AND spi.product_name NOT LIKE '%増連セット%'
    AND spi.product_name NOT LIKE '%片面開閉カバーセット%'
    AND spi.product_name NOT LIKE '%ハコ4色セット%'
    AND spi.product_name NOT LIKE '%ジョイントセット%'
    AND spi.product_name NOT LIKE '%インセットパネル%'
    THEN '除外'
  WHEN spi.product_name LIKE '%SET%' THEN '除外'
  WHEN spi.product_name LIKE '%プラン%' THEN '除外'
  -- 重複SKU
  WHEN dup_ex.sku_id IS NOT NULL THEN '除外'
  ELSE NULL
END AS `除外フラグ`
```

### 除外条件一覧

| 条件 | 理由 |
|---|---|
| SKU_IDが欠番 | 連番出力のため行は存在するがデータなし |
| 削除済みSKU | 論理削除されたSKU |
| 孤立SKU | 現在の属性マスタに存在しないハッシュ |
| 【商品おまかせでおトク】シリーズ | 特殊商品のため除外 |
| 【CLAS SET】シリーズ | セット商品のため除外 |
| エイトレントシリーズ | 外部連携商品のため除外 |
| セット商品 | 複数SKUのセットは個別計算不可 |
| プラン商品 | 価格プランで在庫管理対象外 |
| 重複SKU（rank > 1） | 同一パーツ構成の重複防止 |

**セット商品の例外**:
- 増連セット: デスク増連などの追加パーツ
- 片面開閉カバーセット: ベッドカバーセット
- ハコ4色セット: 収納ボックスセット
- ジョイントセット: 連結パーツセット
- インセットパネル: デスクパネルセット

これらは商品名に「セット」が含まれるが、実質的に単体商品として扱う。

---

## 出力カラム一覧

### 基本情報（1-16）

| # | カラム名 | 説明 | データソース |
|---|---|---|---|
| 1 | 画像リンク | SKU画像のURL | lake.image |
| 2 | 除外フラグ | 除外対象の場合「除外」 | 判定ロジック |
| 3 | SKU_ID | SKU識別子（1〜MAX連番） | all_sku_ids |
| 4 | 商品ID | 商品識別子 | lake.product |
| 5 | 商品IDリンク | 管理画面へのリンク | 生成 |
| 6 | シリーズ名 | シリーズ名称 | lake.series |
| 7 | 商品名 | 商品名称 | lake.product |
| 8 | 属性 | 属性値の連結 | attribute_based_info |
| 9 | カテゴリ | 商品カテゴリ（日本語） | category_mapping |
| 10 | 対象顧客 | 一般顧客/法人顧客 | lake.product_customer |
| 11 | サプライヤー | サプライヤー名 | lake.supplier |
| 12 | 下代 | 下代金額 | attribute_based_info |
| 13 | 上代 | 上代金額 | lake.product.retail_price |
| 14 | 構成パーツid | パーツIDの改行区切りリスト | sku_part_info |
| 15 | 構成パーツ名 | パーツ名の改行区切りリスト | sku_part_info |
| 16 | 構成点数 | 必要数量の改行区切りリスト | sku_part_info |

### 在庫ID（17-29）

| # | カラム名 | 説明 | 形式 |
|---|---|---|---|
| 17-21 | 期末1〜減損済_在庫id_商品 | 商品の在庫ID | [パーツid:倉庫名]在庫id |
| 25-29 | 期末1〜減損済_在庫id_余りパーツ | 余りパーツの在庫ID | [パーツid:倉庫名]在庫id |

### 数量（30-47）

| # | カラム名 | 説明 |
|---|---|---|
| 30-35 | 期末1〜合計_商品数 | 減損予定商品数 |
| 36-41 | 期末1〜合計_余りパーツ数 | 減損予定余りパーツ数 |
| 42-47 | 期末1〜合計_商品+余りパーツ | 合計数 |

### 累計（48-51）

| # | カラム名 | 説明 |
|---|---|---|
| 48-51 | 期末1〜4以降_商品+余りパーツ累計 | 累計減損予定数 |

### 取得原価（52-59）

| # | カラム名 | 説明 |
|---|---|---|
| 52-55 | 期末1〜4以降_減損予定_取得原価 | 期間別取得原価 |
| 56-59 | 期末1〜4以降_減損予定_取得原価_累計 | 累計取得原価 |

### その他（60-64）

| # | カラム名 | 説明 |
|---|---|---|
| 60 | 滞留日数_平均_切上 | 平均滞留日数（切り上げ） |
| 61-64 | 期末1〜4_日付 | 各期末の日付 |

---

## 注意事項・制約

### パフォーマンス

- クエリは複雑なため、全件実行に時間がかかる場合があります
- テスト時は `WHERE sku.id = XX` を追加して実行を推奨

### データの整合性

- `stock.cost`がNULLの場合は0として計算
- 入庫日が`9999-12-31`の場合は資産台帳の入庫日を優先

### 既知の制限

- BigQueryの複雑性制限により、特定のSKUを絞り込んだクエリでも失敗する場合がある
- 大量のCTEを使用しているため、クエリプランニングに時間がかかる

### 今後の拡張候補

- [ ] エリア別の取得原価表示
- [ ] 減損済みの取得原価表示
- [ ] 在庫IDのCSV形式出力オプション

---

## 変更履歴

| 日付 | 内容 |
|---|---|
| 2024-12-23 | v2初版作成（排他カテゴリ方式、動的期末計算） |
| 2024-12-24 | 倉庫名表示追加、CROSS JOIN方式に修正、取得原価カラム追加 |
| 2024-12-24 | 技術仕様書詳細版作成（CTE別解説、重複除外ロジック詳細） |
