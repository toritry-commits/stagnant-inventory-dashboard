-- ============================================================================
-- 余りパーツ「解消提案」レポート SKU単位版 (1行 = SKU x エリア)
-- ============================================================================
-- 元   : sandbox.leftover_reason_report (leftover_reason_report.sql で作成) を集約
-- 内容 : - 相方パーツ (不足パーツ) をセル内改行で1セルにまとめる
--        - 余り在庫ID / 在庫IDリンク / 案件ID / 案件名 / 案件リンク を
--          1行1在庫のセル内改行で同じ順序・同じ件数で出す (紐づき無しは '-')
--        - 案件 = lent_to_b -> contract_proposition_building -> proposition_building
--          -> proposition。在庫ごとに最新 (start_at 降順) の1案件。大半は過去(Ended)案件
-- 実行 : bq query --use_legacy_sql=false --location=asia-northeast1 --project_id=clas-analytics < このファイル
--        (先に leftover_reason_report.sql を実行して詳細テーブルを最新化すること)
-- 出力 : GAS writeLeftoverReportBySku がこれを読んでシートに書き込む
-- ============================================================================

CREATE OR REPLACE TABLE `clas-analytics.sandbox.leftover_reason_report_by_sku` AS
WITH base AS (
  SELECT
    ANY_VALUE(`画像`) AS `画像`,
    `SKU_ID`,
    ANY_VALUE(`商品ID`) AS `商品ID`,
    ANY_VALUE(`商品IDリンク`) AS `商品IDリンク`,
    ANY_VALUE(`シリーズ名`) AS `シリーズ名`,
    ANY_VALUE(`商品名`) AS `商品名`,
    ANY_VALUE(`属性`) AS `属性`,
    ANY_VALUE(`カテゴリ`) AS `カテゴリ`,
    ANY_VALUE(`サプライヤー`) AS `サプライヤー`,
    `エリア`,
    ANY_VALUE(`余りパーツ内訳`) AS `余りパーツ内訳`,
    ANY_VALUE(`余り点数_貸出可能`) AS `余り点数_貸出可能`,
    ANY_VALUE(`余り在庫ID`) AS `余り在庫ID`,
    IFNULL(ANY_VALUE(`配送準備中案件ID`), '-') AS `配送準備中案件ID`,
    IFNULL(ANY_VALUE(`配送準備中案件名`), '-') AS `配送準備中案件名`,
    IFNULL(ANY_VALUE(`配送準備中案件リンク`), '-') AS `配送準備中案件リンク`,
    -- 相方パーツをセル内改行で集約。単価が取れないものは @単価不明
    STRING_AGG(
      CONCAT(
        CAST(`相方パーツID` AS STRING), ':', `相方パーツ名`,
        ' 必要', CAST(`追加必要点数` AS STRING), '点',
        IF(`単価` IS NULL, ' @単価不明', CONCAT(' @', CAST(CAST(`単価` AS INT64) AS STRING), '円'))
      ),
      '\n' ORDER BY `相方パーツID`
    ) AS `相方パーツ内訳`,
    ANY_VALUE(`追加で組める商品数`) AS `追加で組める商品数`,
    SUM(`追加必要点数`) AS `追加必要点数合計`,
    SUM(`必要金額`) AS `必要金額合計`,
    -- 管理対象外内訳はパーツごとに [パーツID] を頭に付けて改行連結
    STRING_AGG(
      IF(`相方の管理対象外内訳` IS NULL, NULL,
        CONCAT('[', CAST(`相方パーツID` AS STRING), '] ', REPLACE(`相方の管理対象外内訳`, '\n', ' / '))),
      '\n' ORDER BY `相方パーツID`
    ) AS `相方の管理対象外内訳`
  FROM `clas-analytics.sandbox.leftover_reason_report`
  GROUP BY `SKU_ID`, `エリア`
),
-- 余り在庫IDを行展開 (pos で元の並び順を保持)
ids AS (
  SELECT
    b.`SKU_ID`,
    b.`エリア`,
    pos,
    CAST(sid AS INT64) AS stock_id
  FROM base b, UNNEST(SPLIT(b.`余り在庫ID`, '\n')) AS sid WITH OFFSET pos
),
-- 在庫ごとの現役案件 (lent_to_b 経由)
-- Ended (利用終了) / Sold / Cancel は紐づき扱いしない。現役 = Preparing / InUse のみ
stock_prop AS (
  SELECT
    d.stock_id,
    p.id AS prop_id,
    p.name AS prop_name
  FROM `clas-analytics.lake.lent_to_b_detail` d
  INNER JOIN `clas-analytics.lake.lent_to_b` b ON b.id = d.lent_to_b_id AND b.deleted_at IS NULL
    AND b.status IN ('Preparing', 'InUse')
    -- 差し替え/返却処理漏れでステータスが残った行を除外 (終了日が過去なら現役扱いしない)
    AND (b.end_at IS NULL OR CAST(b.end_at AS DATE) >= CURRENT_DATE('Asia/Tokyo'))
  INNER JOIN `clas-analytics.lake.contract_proposition_building` cpb
    ON cpb.id = b.contract_proposition_building_id AND cpb.deleted_at IS NULL
  INNER JOIN `clas-analytics.lake.proposition_building` pb
    ON pb.id = cpb.proposition_building_id AND pb.deleted_at IS NULL
  INNER JOIN `clas-analytics.lake.proposition` p ON p.id = pb.proposition_id AND p.deleted_at IS NULL
  WHERE d.deleted_at IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.stock_id ORDER BY b.start_at DESC, b.id DESC) = 1
),
-- 在庫IDと同じ順序・同じ件数で案件情報を改行連結 (紐づき無しは '-')
prop_agg AS (
  SELECT
    i.`SKU_ID`,
    i.`エリア`,
    STRING_AGG(IFNULL(CAST(sp.prop_id AS STRING), '-'), '\n' ORDER BY i.pos) AS `案件ID`,
    STRING_AGG(IFNULL(sp.prop_name, '-'), '\n' ORDER BY i.pos) AS `案件名`,
    STRING_AGG(
      IF(sp.prop_id IS NULL, '-', CONCAT('https://clas.style/admin/proposition/', CAST(sp.prop_id AS STRING))),
      '\n' ORDER BY i.pos
    ) AS `案件リンク`
  FROM ids i
  LEFT JOIN stock_prop sp ON sp.stock_id = i.stock_id
  GROUP BY i.`SKU_ID`, i.`エリア`
)
SELECT
  b.`画像`,
  b.`SKU_ID`,
  b.`商品ID`,
  b.`商品IDリンク`,
  b.`シリーズ名`,
  b.`商品名`,
  b.`属性`,
  b.`カテゴリ`,
  b.`サプライヤー`,
  b.`エリア`,
  b.`余りパーツ内訳`,
  b.`余り点数_貸出可能`,
  b.`余り在庫ID`,
  REGEXP_REPLACE(b.`余り在庫ID`, r'([0-9]+)', r'https://clas.style/admin/stock/\1') AS `余り在庫IDリンク`,
  pa.`案件ID`,
  pa.`案件名`,
  pa.`案件リンク`,
  b.`配送準備中案件ID`,
  b.`配送準備中案件名`,
  b.`配送準備中案件リンク`,
  b.`追加で組める商品数`,
  b.`相方パーツ内訳`,
  b.`追加必要点数合計`,
  b.`必要金額合計`,
  b.`相方の管理対象外内訳`
FROM base b
LEFT JOIN prop_agg pa ON pa.`SKU_ID` = b.`SKU_ID` AND pa.`エリア` = b.`エリア`;

-- 確認用サマリ
SELECT `エリア`, COUNT(*) AS row_count, SUM(`必要金額合計`) AS total_amount,
  COUNTIF(ARRAY_LENGTH(SPLIT(`案件ID`, '\n')) != ARRAY_LENGTH(SPLIT(`余り在庫ID`, '\n'))) AS misaligned
FROM `clas-analytics.sandbox.leftover_reason_report_by_sku`
GROUP BY `エリア`;
