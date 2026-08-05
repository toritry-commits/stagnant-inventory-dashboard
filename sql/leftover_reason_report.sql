-- ============================================================================
-- 余りパーツ「解消提案」レポート (SKU x エリア x 不足相方パーツ)
-- ============================================================================
-- 目的 : 余りパーツが「なぜ余っているか (どの相方パーツが不足か)」と
--        「いくら投資すれば商品に変えられるか」を SKU 軸で出す。
-- 行   : 1行 = SKU x エリア x 不足している相方パーツ
-- 仕様 : - セット数上限 = 余りパーツ(貸出可能)を使い切る商品数の最大値
--        - 単価 = 相方パーツの在庫cost平均 -> 発注明細cost平均 のフォールバック
--        - 必要金額 = 単価 x 追加必要点数 (相方に余りがあれば差し引く)
--        - SKU表示情報 (画像/シリーズ名/属性/カテゴリ/サプライヤー) 付き
--        - 相方の管理対象外(Impossibility)内訳を分類タグ別にセル内改行で表示
-- 実行 : bq query --use_legacy_sql=false --location=asia-northeast1 --project_id=clas-analytics < このファイル
-- 注意 : lake.stock を日付ピンなしで読むため、実行タイミングで数値は変動する (ライブ値)
-- 由来 : leftover_id_count_standalone.sql の余り計算ロジック (1-707行) を流用し、
--        最終集計のみ差し替え。要件: docs/02_requirements/leftover_reason_report_requirements.md
-- ============================================================================

-- ============================================================================
-- sku_stock_ids_by_area (kanto/kansai/kyushu leftover_ids) standalone script
-- lake.* only, no mart.* dependency
-- Derived from sp_sku_inventory_v2.sql -- leftover-ids logic only (minimal)
-- Multi-statement TEMP-table script: heaviest re-referenced CTEs persisted as
-- TEMP tables to keep the query planner within resource limits.
-- CTE bodies are byte-for-byte identical to the original single-query version.
-- ============================================================================

-- Section 1: base_stocks (persisted)
CREATE TEMP TABLE base_stocks AS (
  SELECT
    s.id AS stock_id,
    s.status,
    s.part_id,
    s.cost,
    s.arrival_at,
    w.business_area
  FROM `clas-analytics.lake.stock` s
  INNER JOIN `clas-analytics.lake.part` p ON p.id = s.part_id
    AND p.deleted_at IS NULL
    AND p.name NOT LIKE '%棚卸%'
  INNER JOIN `clas-analytics.lake.part_version` pv ON pv.id = s.part_version_id AND pv.deleted_at IS NULL
  INNER JOIN `clas-analytics.lake.location` l ON l.id = s.location_id
  INNER JOIN `clas-analytics.lake.warehouse` w ON w.id = l.warehouse_id
  LEFT OUTER JOIN (
    SELECT ld.stock_id
    FROM `clas-analytics.lake.lent` lent
    INNER JOIN `clas-analytics.lake.lent_detail` ld ON lent.id = ld.lent_id
    WHERE lent.status IN ('Preparing', 'Adjusting') AND lent.deleted_at IS NULL AND ld.deleted_at IS NULL
    GROUP BY ld.stock_id
  ) to_c ON to_c.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT d.stock_id
    FROM `clas-analytics.lake.lent_to_b` b
    INNER JOIN `clas-analytics.lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing' AND b.deleted_at IS NULL AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT sm.stock_id
    FROM `clas-analytics.lake.staff_memo` sm
    INNER JOIN `clas-analytics.lake.staff_memo_tag` smt ON sm.id = smt.staff_memo_id AND sm.stock_id IS NOT NULL
    INNER JOIN `clas-analytics.lake.memo_tag` mt ON mt.id = smt.memo_tag_id AND mt.id = 64
    WHERE sm.deleted_at IS NULL AND smt.deleted_at IS NULL AND mt.deleted_at IS NULL
    GROUP BY sm.stock_id
  ) tag ON tag.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT pds.stock_id
    FROM `clas-analytics.lake.purchasing_detail_stock` pds
    INNER JOIN `clas-analytics.lake.purchasing_detail` pd ON pds.purchasing_detail_id = pd.id
    INNER JOIN `clas-analytics.lake.purchasing` pp ON pp.id = pd.purchasing_id
    WHERE pds.deleted_at IS NULL AND pd.deleted_at IS NULL AND pp.deleted_at IS NULL
      AND pp.status = 'Done' AND pp.orderer_email = 'info+b-order@clas.style'
    GROUP BY pds.stock_id
  ) purchase ON purchase.stock_id = s.id
  LEFT OUTER JOIN (
    SELECT xss.stock_id
    FROM `clas-analytics.lake.external_sale_stock` xss
    INNER JOIN `clas-analytics.lake.external_sale_product` xsp ON xsp.id = xss.external_sale_product_id
    WHERE xss.deleted_at IS NULL AND xsp.deleted_at IS NULL AND xsp.status != 'Deny'
    GROUP BY xss.stock_id
  ) external_sale ON external_sale.stock_id = s.id
  WHERE s.deleted_at IS NULL
    AND (
      s.status IN ('Ready', 'Waiting')
      OR (s.status = 'Recovery' AND s.part_id NOT IN (7108,7109,7110,7438,7439,7440,7570,7571,7572,7573))
    )
    AND w.available_for_business = TRUE
    AND s._rank_ NOT IN ('R', 'L')
    AND (s._rank_ != 'S' OR s.supplier_id = 320 OR purchase.stock_id IS NOT NULL)
    AND tag.stock_id IS NULL
    AND to_c.stock_id IS NULL
    AND to_b.stock_id IS NULL
    AND external_sale.stock_id IS NULL
    AND w.business_area IS NOT NULL
    AND l.id NOT IN (5400,5402,9389,9390,9521,9522,9523,9629,9702,9703,9951,10120,10355,10374,10415,10820,10948,11022)
    AND (p.inspection_priority != 'NoNeed' OR p.inspection_priority IS NULL)
    AND (pv.inspection_notice NOT LIKE '%検品不要廃棄%' OR pv.inspection_notice IS NULL)
);

-- Section 4: sku_part_detail (persisted). Depends on Sections 2-3 CTEs.
CREATE TEMP TABLE sku_part_detail AS (
  WITH
  -- Section 2: attribute_value_by_type
  attribute_value_by_type AS (
    SELECT
      a.product_id,
      a.type,
      av.id AS av_id,
      av.value AS av_value,
      av.status AS av_status
    FROM `clas-analytics.lake.attribute` a
    INNER JOIN `clas-analytics.lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
  ),
  -- Section 3: sku_logical_mapping
  sku_combinations AS (
    SELECT
      sku.id AS sku_id,
      sku.product_id,
      sku.hash AS physical_sku_hash,
      body.av_id AS body_av_id,
      leg.av_id AS leg_av_id,
      mr.av_id AS mr_av_id,
      mrt.av_id AS mrt_av_id,
      gr.av_id AS gr_av_id
    FROM `clas-analytics.lake.sku` sku
    INNER JOIN `clas-analytics.lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
    LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
    LEFT JOIN attribute_value_by_type leg  ON leg.product_id  = pd.id AND leg.type  = 'Leg'
    LEFT JOIN attribute_value_by_type mr   ON mr.product_id   = pd.id AND mr.type   = 'Mattress'
    LEFT JOIN attribute_value_by_type mrt  ON mrt.product_id  = pd.id AND mrt.type   = 'MattressTopper'
    LEFT JOIN attribute_value_by_type gr   ON gr.product_id   = pd.id AND gr.type   = 'Guarantee'
    WHERE sku.deleted_at IS NULL
      AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element),
        ','
      )))
  ),
  sku_logical_mapping AS (
    SELECT
      sku_id,
      product_id,
      physical_sku_hash,
      body_av_id,
      leg_av_id,
      mr_av_id,
      mrt_av_id,
      gr_av_id,
      TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
        ','
      ))) AS logical_sku_hash,
      MIN(sku_id) OVER (PARTITION BY TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
        ','
      )))) AS representative_sku_id
    FROM sku_combinations
  )
  SELECT
    slm.logical_sku_hash,
    slm.representative_sku_id,
    ANY_VALUE(slm.product_id) AS product_id,
    avp.part_id,
    ANY_VALUE(avp.quantity) AS quantity
  FROM sku_logical_mapping slm
  CROSS JOIN UNNEST([slm.body_av_id, slm.leg_av_id, slm.mr_av_id, slm.mrt_av_id]) AS av_id
  INNER JOIN `clas-analytics.lake.attribute_value_part` avp
    ON avp.attribute_value_id = av_id AND avp.deleted_at IS NULL
  WHERE av_id IS NOT NULL
  GROUP BY slm.logical_sku_hash, slm.representative_sku_id, avp.part_id
);

-- Section 7: business_areas (persisted). Depends on base_stocks (TEMP).
CREATE TEMP TABLE business_areas AS (
  SELECT DISTINCT business_area
  FROM base_stocks
  WHERE business_area IN ('Kanto', 'Kansai', 'Kyushu')
);

-- Section 6: sku_with_priority (persisted). Depends on sku_part_detail (TEMP)
-- and Section 3/5 CTEs.
CREATE TEMP TABLE sku_with_priority AS (
  WITH
  -- Section 2: attribute_value_by_type
  attribute_value_by_type AS (
    SELECT
      a.product_id,
      a.type,
      av.id AS av_id,
      av.value AS av_value,
      av.status AS av_status
    FROM `clas-analytics.lake.attribute` a
    INNER JOIN `clas-analytics.lake.attribute_value` av ON a.id = av.attribute_id
    WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
  ),
  -- Section 3: sku_logical_mapping
  sku_combinations AS (
    SELECT
      sku.id AS sku_id,
      sku.product_id,
      sku.hash AS physical_sku_hash,
      body.av_id AS body_av_id,
      leg.av_id AS leg_av_id,
      mr.av_id AS mr_av_id,
      mrt.av_id AS mrt_av_id,
      gr.av_id AS gr_av_id
    FROM `clas-analytics.lake.sku` sku
    INNER JOIN `clas-analytics.lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
    LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
    LEFT JOIN attribute_value_by_type leg  ON leg.product_id  = pd.id AND leg.type  = 'Leg'
    LEFT JOIN attribute_value_by_type mr   ON mr.product_id   = pd.id AND mr.type   = 'Mattress'
    LEFT JOIN attribute_value_by_type mrt  ON mrt.product_id  = pd.id AND mrt.type   = 'MattressTopper'
    LEFT JOIN attribute_value_by_type gr   ON gr.product_id   = pd.id AND gr.type   = 'Guarantee'
    WHERE sku.deleted_at IS NULL
      AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element),
        ','
      )))
  ),
  sku_logical_mapping AS (
    SELECT
      sku_id,
      product_id,
      physical_sku_hash,
      body_av_id,
      leg_av_id,
      mr_av_id,
      mrt_av_id,
      gr_av_id,
      TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
        ','
      ))) AS logical_sku_hash,
      MIN(sku_id) OVER (PARTITION BY TO_HEX(SHA1(ARRAY_TO_STRING(
        ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
        ','
      )))) AS representative_sku_id
    FROM sku_combinations
  ),
  -- Section 5: excluded_logical_skus
  excluded_logical_skus AS (
    SELECT DISTINCT slm.logical_sku_hash
    FROM sku_logical_mapping slm
    INNER JOIN `clas-analytics.lake.product` pd ON pd.id = slm.product_id AND pd.deleted_at IS NULL
    INNER JOIN `clas-analytics.lake.series` se ON se.id = pd.series_id AND se.deleted_at IS NULL
    WHERE
      (pd.name LIKE '%セット%'
        AND pd.name NOT LIKE '%増連セット%'
        AND pd.name NOT LIKE '%片面開閉カバーセット%'
        AND pd.name NOT LIKE '%ハコ4色セット%'
        AND pd.name NOT LIKE '%ジョイントセット%'
        AND pd.name NOT LIKE '%インセットパネル%')
      OR pd.name LIKE '%SET%'
      OR pd.name LIKE '%プラン%'
      OR se.name LIKE '%【商品おまかせでおトク】%'
      OR se.name LIKE '%【CLAS SET】%'
      OR se.name LIKE '%エイトレント%'
  ),
  -- Section 6: rental count / priority
  rental_base AS (
    SELECT DISTINCT
      s.part_id,
      ld.lent_id
    FROM `clas-analytics.lake.lent_detail` ld
    INNER JOIN `clas-analytics.lake.lent` lent ON lent.id = ld.lent_id
    INNER JOIN `clas-analytics.lake.stock` s ON s.id = ld.stock_id
    WHERE lent.status NOT IN ('Cancel', 'PurchaseFailed')
      AND lent.deleted_at IS NULL
      AND ld.deleted_at IS NULL
      AND CAST(lent.created_at AS DATE) >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 12 MONTH)
  ),
  part_rental_count AS (
    SELECT
      part_id,
      COUNT(DISTINCT lent_id) AS rental_count
    FROM rental_base
    GROUP BY part_id
  ),
  sku_rental_count AS (
    SELECT
      spd.logical_sku_hash,
      ANY_VALUE(spd.representative_sku_id) AS representative_sku_id,
      ANY_VALUE(spd.product_id) AS product_id,
      SUM(COALESCE(prc.rental_count, 0)) AS rental_count
    FROM sku_part_detail spd
    LEFT JOIN part_rental_count prc ON spd.part_id = prc.part_id
    WHERE spd.logical_sku_hash NOT IN (SELECT logical_sku_hash FROM excluded_logical_skus)
    GROUP BY spd.logical_sku_hash
  )
  SELECT
    logical_sku_hash,
    representative_sku_id,
    product_id,
    rental_count,
    DENSE_RANK() OVER (
      ORDER BY rental_count DESC, logical_sku_hash ASC
    ) AS sku_priority
  FROM sku_rental_count
);

-- Section 8-9: sku_inventory_by_area (persisted). Depends on sku_part_detail,
-- sku_with_priority, business_areas, base_stocks (all TEMP).
CREATE TEMP TABLE sku_inventory_by_area AS (
  WITH
  -- Section 7: part inventory aggregates
  part_available_inventory AS (
    SELECT
      part_id,
      business_area,
      COUNT(*) AS available_count
    FROM base_stocks
    WHERE status IN ('Ready', 'Waiting')
    GROUP BY part_id, business_area
  ),
  part_recovery_inventory AS (
    SELECT
      part_id,
      business_area,
      COUNT(*) AS recovery_count
    FROM base_stocks
    WHERE status = 'Recovery'
    GROUP BY part_id, business_area
  ),
  -- Section 8: available allocation
  sku_part_with_available AS (
    SELECT
      spd.logical_sku_hash,
      spd.representative_sku_id,
      spd.product_id,
      spd.part_id,
      spd.quantity AS required_qty,
      swp.sku_priority,
      ba.business_area,
      COALESCE(pai.available_count, 0) AS available_count,
      CAST(FLOOR(COALESCE(pai.available_count, 0) / spd.quantity) AS INT64) AS max_units_from_part
    FROM sku_part_detail spd
    INNER JOIN sku_with_priority swp ON spd.logical_sku_hash = swp.logical_sku_hash
    CROSS JOIN business_areas ba
    LEFT JOIN part_available_inventory pai
      ON spd.part_id = pai.part_id AND ba.business_area = pai.business_area
  ),
  sku_assemblable_available AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      MIN(max_units_from_part) AS max_assemblable
    FROM sku_part_with_available
    GROUP BY logical_sku_hash, representative_sku_id, product_id, business_area, sku_priority
    HAVING MIN(max_units_from_part) > 0
  ),
  sku_cumulative_consumption AS (
    SELECT
      saa.logical_sku_hash,
      saa.representative_sku_id,
      saa.product_id,
      saa.business_area,
      saa.sku_priority,
      saa.max_assemblable,
      spd.part_id,
      spd.quantity AS required_qty,
      SUM(saa.max_assemblable * spd.quantity) OVER (
        PARTITION BY spd.part_id, saa.business_area
        ORDER BY saa.sku_priority, saa.logical_sku_hash
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_required,
      pai.available_count
    FROM sku_assemblable_available saa
    INNER JOIN sku_part_detail spd ON saa.logical_sku_hash = spd.logical_sku_hash
    LEFT JOIN part_available_inventory pai
      ON spd.part_id = pai.part_id AND saa.business_area = pai.business_area
  ),
  sku_part_allocatable_avail AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      max_assemblable,
      part_id,
      required_qty,
      cumulative_required,
      available_count,
      CASE
        WHEN cumulative_required <= COALESCE(available_count, 0) THEN max_assemblable
        WHEN cumulative_required - max_assemblable * required_qty < COALESCE(available_count, 0) THEN
          CAST(FLOOR((COALESCE(available_count, 0) - (cumulative_required - max_assemblable * required_qty)) / required_qty) AS INT64)
        ELSE 0
      END AS allocatable_units
    FROM sku_cumulative_consumption
  ),
  sku_final_available AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      MIN(allocatable_units) AS final_units
    FROM sku_part_allocatable_avail
    GROUP BY logical_sku_hash, representative_sku_id, product_id, business_area, sku_priority
    HAVING MIN(allocatable_units) > 0
  ),
  -- Section 9: recovery allocation + sku_inventory_by_area
  consumed_by_available AS (
    SELECT
      spd.part_id,
      sfa.business_area,
      SUM(sfa.final_units * spd.quantity) AS consumed
    FROM sku_final_available sfa
    INNER JOIN sku_part_detail spd ON sfa.logical_sku_hash = spd.logical_sku_hash
    GROUP BY spd.part_id, sfa.business_area
  ),
  remaining_after_available AS (
    SELECT
      COALESCE(pai.part_id, pri.part_id) AS part_id,
      COALESCE(pai.business_area, pri.business_area) AS business_area,
      GREATEST(
        COALESCE(pai.available_count, 0)
        + COALESCE(pri.recovery_count, 0)
        - COALESCE(cba.consumed, 0),
        0
      ) AS remaining_count
    FROM part_available_inventory pai
    FULL OUTER JOIN part_recovery_inventory pri
      ON pai.part_id = pri.part_id AND pai.business_area = pri.business_area
    LEFT JOIN consumed_by_available cba
      ON COALESCE(pai.part_id, pri.part_id) = cba.part_id
      AND COALESCE(pai.business_area, pri.business_area) = cba.business_area
  ),
  sku_part_with_remaining AS (
    SELECT
      spd.logical_sku_hash,
      spd.representative_sku_id,
      spd.product_id,
      spd.part_id,
      spd.quantity AS required_qty,
      swp.sku_priority,
      ba.business_area,
      COALESCE(raa.remaining_count, 0) AS remaining_count,
      CAST(FLOOR(COALESCE(raa.remaining_count, 0) / spd.quantity) AS INT64) AS max_units_from_part
    FROM sku_part_detail spd
    INNER JOIN sku_with_priority swp ON spd.logical_sku_hash = swp.logical_sku_hash
    CROSS JOIN business_areas ba
    LEFT JOIN remaining_after_available raa
      ON spd.part_id = raa.part_id AND ba.business_area = raa.business_area
  ),
  sku_assemblable_total AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      MIN(max_units_from_part) AS max_assemblable
    FROM sku_part_with_remaining
    GROUP BY logical_sku_hash, representative_sku_id, product_id, business_area, sku_priority
    HAVING MIN(max_units_from_part) > 0
  ),
  sku_cumulative_total AS (
    SELECT
      sat.logical_sku_hash,
      sat.representative_sku_id,
      sat.product_id,
      sat.business_area,
      sat.sku_priority,
      sat.max_assemblable,
      spd.part_id,
      spd.quantity AS required_qty,
      SUM(sat.max_assemblable * spd.quantity) OVER (
        PARTITION BY spd.part_id, sat.business_area
        ORDER BY sat.sku_priority, sat.logical_sku_hash
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_required,
      raa.remaining_count
    FROM sku_assemblable_total sat
    INNER JOIN sku_part_detail spd ON sat.logical_sku_hash = spd.logical_sku_hash
    LEFT JOIN remaining_after_available raa
      ON spd.part_id = raa.part_id AND sat.business_area = raa.business_area
  ),
  sku_part_allocatable_total AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      max_assemblable,
      part_id,
      required_qty,
      cumulative_required,
      remaining_count,
      CASE
        WHEN cumulative_required <= COALESCE(remaining_count, 0) THEN max_assemblable
        WHEN cumulative_required - max_assemblable * required_qty < COALESCE(remaining_count, 0) THEN
          CAST(FLOOR((COALESCE(remaining_count, 0) - (cumulative_required - max_assemblable * required_qty)) / required_qty) AS INT64)
        ELSE 0
      END AS allocatable_units
    FROM sku_cumulative_total
  ),
  sku_final_total AS (
    SELECT
      logical_sku_hash,
      representative_sku_id,
      product_id,
      business_area,
      sku_priority,
      MIN(allocatable_units) AS final_units
    FROM sku_part_allocatable_total
    GROUP BY logical_sku_hash, representative_sku_id, product_id, business_area, sku_priority
  )
  SELECT
    COALESCE(sft.logical_sku_hash, sfa.logical_sku_hash) AS logical_sku_hash,
    COALESCE(sft.representative_sku_id, sfa.representative_sku_id) AS representative_sku_id,
    COALESCE(sft.product_id, sfa.product_id) AS product_id,
    COALESCE(sft.business_area, sfa.business_area) AS business_area,
    COALESCE(sfa.final_units, 0) AS available_units,
    GREATEST(COALESCE(sft.final_units, 0) - COALESCE(sfa.final_units, 0), 0) AS recovery_units
  FROM sku_final_total sft
  FULL OUTER JOIN sku_final_available sfa
    ON sft.logical_sku_hash = sfa.logical_sku_hash
    AND sft.business_area = sfa.business_area
);

-- Section 12.5a: top_sku_per_part_area (persisted). Depends on sku_part_detail,
-- sku_with_priority, business_areas (all TEMP).
CREATE TEMP TABLE top_sku_per_part_area AS (
  SELECT
    part_id,
    business_area,
    ARRAY_AGG(logical_sku_hash ORDER BY sku_priority, logical_sku_hash LIMIT 1)[OFFSET(0)] AS top_sku_hash
  FROM (
    SELECT DISTINCT
      spd.part_id,
      ba.business_area,
      spd.logical_sku_hash,
      swp.sku_priority
    FROM sku_part_detail spd
    INNER JOIN sku_with_priority swp ON spd.logical_sku_hash = swp.logical_sku_hash
    CROSS JOIN business_areas ba
  )
  GROUP BY part_id, business_area
);

-- ============================================================================
-- Section 13: SKU表示情報 (画像 / シリーズ名 / 属性 / カテゴリ / サプライヤー)
--   leftover_id_count_faithful.sql の Section 12 と
--   stagnant_inventory_dashboard_v3.sql のカテゴリ変換を流用
-- ============================================================================
CREATE TEMP TABLE part_supplier AS
SELECT
  pv.part_id,
  ANY_VALUE(sup.name) AS supplier_name
FROM `clas-analytics.lake.part_version` pv
INNER JOIN (SELECT part_id, MAX(version) AS max_ver FROM `clas-analytics.lake.part_version` GROUP BY part_id) pv2
  ON pv.version = pv2.max_ver AND pv.part_id = pv2.part_id
INNER JOIN `clas-analytics.lake.supplier` sup ON sup.id = pv.supplier_id
WHERE pv.deleted_at IS NULL AND sup.deleted_at IS NULL
GROUP BY pv.part_id;

CREATE TEMP TABLE sku_info AS
WITH
attribute_value_by_type AS (
  SELECT
    a.product_id,
    a.type,
    av.id AS av_id,
    av.value AS av_value,
    av.status AS av_status
  FROM `clas-analytics.lake.attribute` a
  INNER JOIN `clas-analytics.lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
),
sku_combinations AS (
  SELECT
    sku.id AS sku_id,
    sku.product_id,
    body.av_id AS body_av_id,
    leg.av_id AS leg_av_id,
    mr.av_id AS mr_av_id,
    mrt.av_id AS mrt_av_id
  FROM `clas-analytics.lake.sku` sku
  INNER JOIN `clas-analytics.lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg  ON leg.product_id  = pd.id AND leg.type  = 'Leg'
  LEFT JOIN attribute_value_by_type mr   ON mr.product_id   = pd.id AND mr.type   = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt  ON mrt.product_id  = pd.id AND mrt.type   = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr   ON gr.product_id   = pd.id AND gr.type   = 'Guarantee'
  WHERE sku.deleted_at IS NULL
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(
      ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element),
      ','
    )))
),
sku_logical_mapping AS (
  SELECT
    sku_id,
    product_id,
    body_av_id,
    leg_av_id,
    mr_av_id,
    mrt_av_id,
    TO_HEX(SHA1(ARRAY_TO_STRING(
      ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
      ','
    ))) AS logical_sku_hash
  FROM sku_combinations
),
av_display AS (
  SELECT
    av_id,
    CONCAT(
      CASE type
        WHEN 'Body' THEN '本体'
        WHEN 'Leg' THEN '脚'
        WHEN 'Mattress' THEN 'マットレス'
        WHEN 'MattressTopper' THEN '寝心地オプション'
        WHEN 'Guarantee' THEN '補償'
        ELSE type
      END,
      ': ',
      av_value
    ) AS display_value
  FROM attribute_value_by_type
),
category_mapping AS (
  SELECT category_en, category_ja FROM UNNEST([
    STRUCT('Sofa' AS category_en, 'ソファ' AS category_ja),
    STRUCT('Bed', 'ベッド・寝具'),
    STRUCT('Chair', 'チェア'),
    STRUCT('WorkSeat', 'オフィスチェア'),
    STRUCT('Table', 'テーブル'),
    STRUCT('Dining', 'ダイニング'),
    STRUCT('Desk', 'デスク'),
    STRUCT('Storage', '収納'),
    STRUCT('TvBoard', 'テレビ台'),
    STRUCT('Lighting', '照明'),
    STRUCT('Fabric', 'ファブリック'),
    STRUCT('RugAndCarpet', 'ラグ・カーペット'),
    STRUCT('Curtain', 'カーテン'),
    STRUCT('KidsAndBabies', 'キッズ&ベビー'),
    STRUCT('InteriorGreen', '観葉植物'),
    STRUCT('Outdoor', 'アウトドア'),
    STRUCT('OtherFurniture', 'その他の家具'),
    STRUCT('Washer', '洗濯機'),
    STRUCT('Refrigerator', '冷蔵庫'),
    STRUCT('Microwave', '電子レンジ'),
    STRUCT('KitchenAppliance', 'キッチン家電'),
    STRUCT('Television', 'テレビ'),
    STRUCT('Cleaner', '掃除機'),
    STRUCT('PcPeripherals', 'PC周辺機器'),
    STRUCT('AirConditioning', '空調家電'),
    STRUCT('Beauty', '美容家電'),
    STRUCT('OtherElectronics', 'その他の家電'),
    STRUCT('Babycrib', 'ベビーベッド'),
    STRUCT('BabyBedding', 'ベビー寝具'),
    STRUCT('Mobile', 'モビール'),
    STRUCT('Bouncer', 'バウンサー'),
    STRUCT('BabyChair', 'ベビーチェア'),
    STRUCT('FamilyAppliance', 'ファミリー家電'),
    STRUCT('FamilyInterior', 'インテリア'),
    STRUCT('BabyCare', 'ベビーケア'),
    STRUCT('ChildSeat', 'チャイルドシート'),
    STRUCT('Fitness', 'フィットネス'),
    STRUCT('HighLowChair', 'ハイローチェア'),
    STRUCT('HomeGoods', 'ホームグッズ'),
    STRUCT('Mattress', 'マットレス'),
    STRUCT('OfficeChair', 'オフィスチェア'),
    STRUCT('OfficeDesk', 'オフィスデスク'),
    STRUCT('OfficeInterior', 'オフィスインテリア'),
    STRUCT('OfficePcPeripherals', 'オフィスPC周辺機器'),
    STRUCT('OfficeSofa', 'オフィスソファ'),
    STRUCT('OfficeStorage', 'オフィス収納'),
    STRUCT('OfficeTable', 'オフィステーブル'),
    STRUCT('Partition', 'パーティション'),
    STRUCT('Stroller', 'ベビーカー'),
    STRUCT('Travel', 'トラベル')
  ])
),
per_logical AS (
  SELECT
    logical_sku_hash,
    ANY_VALUE(product_id) AS product_id,
    ANY_VALUE(body_av_id) AS body_av_id,
    ANY_VALUE(leg_av_id) AS leg_av_id,
    ANY_VALUE(mr_av_id) AS mr_av_id,
    ANY_VALUE(mrt_av_id) AS mrt_av_id
  FROM sku_logical_mapping
  GROUP BY logical_sku_hash
),
sku_supplier AS (
  SELECT
    spd.logical_sku_hash,
    STRING_AGG(DISTINCT ps.supplier_name, ',' ORDER BY ps.supplier_name) AS supplier
  FROM sku_part_detail spd
  LEFT JOIN part_supplier ps ON ps.part_id = spd.part_id
  GROUP BY spd.logical_sku_hash
)
SELECT
  pl.logical_sku_hash,
  se.name AS series_name,
  pd.name AS product_name,
  COALESCE(cm.category_ja, se.category) AS category_name,
  ARRAY_TO_STRING(
    [body.display_value, leg.display_value, mr.display_value, mrt.display_value],
    ' '
  ) AS attribute_value,
  ss.supplier,
  -- URL形式は sandbox.assemblable_nonop_by_sku の実績フォーマット (拡張子なし + resizeパラメータ) に合わせる
  IF(image.id IS NULL, NULL,
    CONCAT(
      'https://clas.style/images/sku/',
      CAST(image.ref_id AS STRING),
      '/',
      LPAD(CAST(image.id AS STRING), 10, '0'),
      '_',
      TO_BASE64(image.hash),
      '?resize.width=1080&resize.height=1080&resize.fit=cover&format=webp'
    )
  ) AS image_url
FROM per_logical pl
INNER JOIN `clas-analytics.lake.product` pd ON pd.id = pl.product_id AND pd.deleted_at IS NULL
INNER JOIN `clas-analytics.lake.series` se ON se.id = pd.series_id AND se.deleted_at IS NULL
LEFT JOIN category_mapping cm ON cm.category_en = se.category
LEFT JOIN av_display body ON body.av_id = pl.body_av_id
LEFT JOIN av_display leg  ON leg.av_id  = pl.leg_av_id
LEFT JOIN av_display mr   ON mr.av_id   = pl.mr_av_id
LEFT JOIN av_display mrt  ON mrt.av_id  = pl.mrt_av_id
LEFT JOIN sku_supplier ss ON ss.logical_sku_hash = pl.logical_sku_hash
LEFT JOIN `clas-analytics.lake.image` image
  ON image.type = 'Sku'
  AND image.ref_id = pl.product_id
  AND image.hint = TO_HEX(SHA1(ARRAY_TO_STRING(
    ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([pl.body_av_id, pl.leg_av_id, pl.mr_av_id, pl.mrt_av_id]) AS element ORDER BY element),
    ','
  )))
  AND image.deleted_at IS NULL
-- 画像が複数ヒットしても1SKU=1行を保証
QUALIFY ROW_NUMBER() OVER (PARTITION BY pl.logical_sku_hash ORDER BY image.id) = 1;

-- ============================================================================
-- Section 14: 配送準備中案件 (SKUレベル)
--   同じ論理SKUの lent_to_b が Preparing (配送準備中) の案件を列挙する。
--   余り在庫そのものの割当ではない (割当済み在庫は余りから除外されている) が、
--   「この余りの SKU は近々案件で使われる」という購買判断の材料になる。
--   エリアは proposition_building.area の欠損が多いため絞らない (全エリア)。
-- ============================================================================
CREATE TEMP TABLE sku_preparing_props AS
WITH
attribute_value_by_type AS (
  SELECT
    a.product_id,
    a.type,
    av.id AS av_id
  FROM `clas-analytics.lake.attribute` a
  INNER JOIN `clas-analytics.lake.attribute_value` av ON a.id = av.attribute_id
  WHERE a.deleted_at IS NULL AND av.deleted_at IS NULL
),
sku_combinations AS (
  SELECT
    sku.id AS sku_id,
    body.av_id AS body_av_id,
    leg.av_id AS leg_av_id,
    mr.av_id AS mr_av_id,
    mrt.av_id AS mrt_av_id
  FROM `clas-analytics.lake.sku` sku
  INNER JOIN `clas-analytics.lake.product` pd ON pd.id = sku.product_id AND pd.deleted_at IS NULL
  LEFT JOIN attribute_value_by_type body ON body.product_id = pd.id AND body.type = 'Body'
  LEFT JOIN attribute_value_by_type leg  ON leg.product_id  = pd.id AND leg.type  = 'Leg'
  LEFT JOIN attribute_value_by_type mr   ON mr.product_id   = pd.id AND mr.type   = 'Mattress'
  LEFT JOIN attribute_value_by_type mrt  ON mrt.product_id  = pd.id AND mrt.type   = 'MattressTopper'
  LEFT JOIN attribute_value_by_type gr   ON gr.product_id   = pd.id AND gr.type   = 'Guarantee'
  WHERE sku.deleted_at IS NULL
    AND sku.hash = TO_HEX(SHA1(ARRAY_TO_STRING(
      ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body.av_id, leg.av_id, mr.av_id, mrt.av_id, gr.av_id]) AS element ORDER BY element),
      ','
    )))
),
sku_logical_mapping AS (
  SELECT
    sku_id,
    TO_HEX(SHA1(ARRAY_TO_STRING(
      ARRAY(SELECT CAST(element AS STRING) FROM UNNEST([body_av_id, leg_av_id, mr_av_id, mrt_av_id]) AS element ORDER BY element),
      ','
    ))) AS logical_sku_hash
  FROM sku_combinations
),
prep AS (
  SELECT DISTINCT
    slm.logical_sku_hash,
    p.id AS prop_id,
    p.name AS prop_name
  FROM sku_logical_mapping slm
  INNER JOIN `clas-analytics.lake.lent_to_b` b ON SAFE_CAST(b.sku_id AS INT64) = slm.sku_id
    AND b.deleted_at IS NULL AND b.status = 'Preparing'
  INNER JOIN `clas-analytics.lake.contract_proposition_building` cpb
    ON cpb.id = b.contract_proposition_building_id AND cpb.deleted_at IS NULL
  INNER JOIN `clas-analytics.lake.proposition_building` pb
    ON pb.id = cpb.proposition_building_id AND pb.deleted_at IS NULL
  INNER JOIN `clas-analytics.lake.proposition` p ON p.id = pb.proposition_id AND p.deleted_at IS NULL
)
SELECT
  logical_sku_hash,
  STRING_AGG(CAST(prop_id AS STRING), '\n' ORDER BY prop_id) AS prep_ids,
  STRING_AGG(prop_name, '\n' ORDER BY prop_id) AS prep_names,
  STRING_AGG(CONCAT('https://clas.style/admin/proposition/', CAST(prop_id AS STRING)), '\n' ORDER BY prop_id) AS prep_links
FROM prep
GROUP BY logical_sku_hash;

-- ============================================================================
-- Final statement: sandbox.leftover_reason_report テーブルを作成 (GASがこれを読んでスプシに出力)
-- ============================================================================
CREATE OR REPLACE TABLE `clas-analytics.sandbox.leftover_reason_report` AS
WITH
-- Section 1.5: ordered_stocks
ordered_stocks AS (
  SELECT
    s.id AS stock_id,
    s.part_id,
    w.business_area
  FROM `clas-analytics.lake.stock` s
  INNER JOIN `clas-analytics.lake.location` l ON l.id = s.location_id
  INNER JOIN `clas-analytics.lake.warehouse` w ON w.id = l.warehouse_id
  LEFT OUTER JOIN (
    SELECT d.stock_id
    FROM `clas-analytics.lake.lent_to_b` b
    INNER JOIN `clas-analytics.lake.lent_to_b_detail` d ON b.id = d.lent_to_b_id
    WHERE b.status = 'Preparing'
      AND b.deleted_at IS NULL
      AND d.deleted_at IS NULL
    GROUP BY d.stock_id
  ) to_b ON to_b.stock_id = s.id
  WHERE s.deleted_at IS NULL
    AND s.status = 'Ordered'
    AND s.inspected_at IS NULL
    AND w.business_area IN ('Kanto', 'Kansai', 'Kyushu')
    AND to_b.stock_id IS NULL
),
top_sku_ord_per_part_top AS (
  SELECT
    ts.top_sku_hash AS logical_sku_hash,
    spd.part_id,
    spd.quantity,
    ts.business_area,
    COUNT(*) AS ordered_count
  FROM ordered_stocks os
  INNER JOIN top_sku_per_part_area ts
    ON ts.part_id = os.part_id AND ts.business_area = os.business_area
  INNER JOIN sku_part_detail spd
    ON spd.logical_sku_hash = ts.top_sku_hash AND spd.part_id = os.part_id
  GROUP BY ts.top_sku_hash, spd.part_id, spd.quantity, ts.business_area
),
top_sku_ord_sku_per_part AS (
  SELECT
    spd.logical_sku_hash,
    spd.part_id,
    ba.business_area,
    CAST(FLOOR(COALESCE(opt.ordered_count, 0) / spd.quantity) AS INT64) AS ordered_units
  FROM sku_part_detail spd
  CROSS JOIN business_areas ba
  LEFT JOIN top_sku_ord_per_part_top opt
    ON opt.logical_sku_hash = spd.logical_sku_hash
    AND opt.part_id = spd.part_id
    AND opt.business_area = ba.business_area
),
top_sku_ordered_assemblable AS (
  SELECT
    logical_sku_hash, business_area,
    MIN(ordered_units) AS top_sku_ordered_units
  FROM top_sku_ord_sku_per_part
  GROUP BY logical_sku_hash, business_area
),
stock_sequence AS (
  SELECT stock_id, part_id, business_area, status,
    ROW_NUMBER() OVER (
      PARTITION BY part_id, business_area
      ORDER BY
        CASE status WHEN 'Ready' THEN 1 WHEN 'Waiting' THEN 2 WHEN 'Recovery' THEN 3 ELSE 4 END,
        cost ASC NULLS LAST,
        stock_id ASC
    ) AS stock_order
  FROM (
    SELECT bs.stock_id, bs.part_id, bs.business_area, bs.status, bs.cost
    FROM base_stocks bs
    UNION ALL
    SELECT os.stock_id, os.part_id, os.business_area, 'Ordered' AS status, NULL AS cost
    FROM ordered_stocks os
  )
),
sku_part_stock_range_base_need AS (
  SELECT
    spd.logical_sku_hash,
    ba.business_area,
    swp.sku_priority,
    spd.part_id,
    spd.quantity,
    COALESCE(sib.available_units, 0) + COALESCE(sib.recovery_units, 0) AS phys_units,
    COALESCE(tsoa.top_sku_ordered_units, 0) AS ordered_units
  FROM sku_part_detail spd
  INNER JOIN sku_with_priority swp ON spd.logical_sku_hash = swp.logical_sku_hash
  CROSS JOIN business_areas ba
  LEFT JOIN sku_inventory_by_area sib
    ON sib.logical_sku_hash = spd.logical_sku_hash AND sib.business_area = ba.business_area
  LEFT JOIN top_sku_ordered_assemblable tsoa
    ON tsoa.logical_sku_hash = spd.logical_sku_hash AND tsoa.business_area = ba.business_area
),
sku_part_stock_range AS (
  SELECT
    logical_sku_hash, business_area, sku_priority, part_id,
    (phys_units + ordered_units) * quantity AS need,
    COALESCE(SUM((phys_units + ordered_units) * quantity) OVER (
      PARTITION BY part_id, business_area
      ORDER BY sku_priority, logical_sku_hash
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ), 0) AS start_offset,
    SUM((phys_units + ordered_units) * quantity) OVER (
      PARTITION BY part_id, business_area
      ORDER BY sku_priority, logical_sku_hash
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS end_offset
  FROM sku_part_stock_range_base_need
  WHERE phys_units + ordered_units > 0
),
stock_assigned AS (
  SELECT
    ss.stock_id,
    ss.part_id,
    ss.business_area,
    ss.status,
    ss.stock_order,
    spsr.logical_sku_hash AS assigned_sku_hash
  FROM stock_sequence ss
  LEFT JOIN sku_part_stock_range spsr
    ON spsr.part_id = ss.part_id
    AND spsr.business_area = ss.business_area
    AND ss.stock_order > spsr.start_offset
    AND ss.stock_order <= spsr.end_offset
),
stock_id_with_status AS (
  SELECT
    sa.assigned_sku_hash AS logical_sku_hash,
    sa.part_id,
    sa.business_area,
    CASE sa.status
      WHEN 'Ready' THEN '貸出可能'
      WHEN 'Waiting' THEN '貸出可能'
      WHEN 'Recovery' THEN 'リカバリー中'
      WHEN 'Ordered' THEN '発注済未着'
    END AS status_label,
    CAST(sa.stock_id AS STRING) AS labeled_id,
    TRUE AS is_assembled,
    sa.stock_order
  FROM stock_assigned sa
  WHERE sa.assigned_sku_hash IS NOT NULL
  UNION ALL
  SELECT
    ts.top_sku_hash AS logical_sku_hash,
    sa.part_id,
    sa.business_area,
    CASE sa.status
      WHEN 'Ready' THEN '貸出可能'
      WHEN 'Waiting' THEN '貸出可能'
      WHEN 'Recovery' THEN 'リカバリー中'
      WHEN 'Ordered' THEN '発注済未着'
    END AS status_label,
    CAST(sa.stock_id AS STRING) AS labeled_id,
    FALSE AS is_assembled,
    sa.stock_order + 1000000 AS stock_order
  FROM stock_assigned sa
  INNER JOIN top_sku_per_part_area ts
    ON ts.part_id = sa.part_id AND ts.business_area = sa.business_area
  WHERE sa.assigned_sku_hash IS NULL
),
-- ============================================================================
-- ここから解消提案レポート固有のロジック
-- ============================================================================
-- SKU x エリア x パーツ別の余り点数 (ステータス別)
leftover_per_sku_part AS (
  SELECT
    logical_sku_hash,
    business_area,
    part_id,
    COUNTIF(status_label = '貸出可能') AS leftover_available,
    COUNTIF(status_label = 'リカバリー中') AS leftover_recovery,
    COUNTIF(status_label = '発注済未着') AS leftover_ordered,
    -- 貸出可能の余り在庫IDを改行区切りで保持 (SKU単位シート用)
    STRING_AGG(IF(status_label = '貸出可能', labeled_id, NULL), '\n' ORDER BY stock_order) AS available_ids
  FROM stock_id_with_status
  WHERE NOT is_assembled
  GROUP BY logical_sku_hash, business_area, part_id
),
-- 目標セット数 = 余り(貸出可能)を使い切る商品数の最大値
sku_target_sets AS (
  SELECT
    l.logical_sku_hash,
    l.business_area,
    MAX(CAST(FLOOR(l.leftover_available / spd.quantity) AS INT64)) AS target_sets
  FROM leftover_per_sku_part l
  INNER JOIN sku_part_detail spd
    ON spd.logical_sku_hash = l.logical_sku_hash AND spd.part_id = l.part_id
  WHERE l.leftover_available > 0
  GROUP BY l.logical_sku_hash, l.business_area
),
-- SKU x エリアごとの余りパーツ内訳 (表示用)
sku_leftover_summary AS (
  SELECT
    l.logical_sku_hash,
    l.business_area,
    STRING_AGG(
      IF(l.leftover_available > 0,
        CONCAT(CAST(l.part_id AS STRING), ':', p.name, ' x', CAST(l.leftover_available AS STRING)),
        NULL),
      '\n' ORDER BY l.part_id) AS leftover_parts_desc,
    SUM(l.leftover_available) AS leftover_available_total,
    SUM(l.leftover_recovery) AS leftover_recovery_total,
    SUM(l.leftover_ordered) AS leftover_ordered_total,
    STRING_AGG(IF(l.leftover_available > 0, l.available_ids, NULL), '\n' ORDER BY l.part_id) AS leftover_ids
  FROM leftover_per_sku_part l
  INNER JOIN `clas-analytics.lake.part` p ON p.id = l.part_id
  GROUP BY l.logical_sku_hash, l.business_area
),
-- 相方パーツの単価: (1) 在庫レコードの cost 平均 -> (2) 発注明細の cost 平均 の順で採用
-- 在庫ゼロだから不足しているパーツが多く、stock.cost だけでは単価が取れないため
part_stock_cost AS (
  SELECT
    part_id,
    ROUND(AVG(cost), 0) AS avg_cost
  FROM `clas-analytics.lake.stock`
  WHERE deleted_at IS NULL
    AND cost IS NOT NULL AND cost > 0
  GROUP BY part_id
),
part_po_cost AS (
  SELECT
    pv.part_id,
    ROUND(AVG(pd.cost), 0) AS avg_cost  -- purchasing_detail.cost は単価 (stock.cost 平均と一致確認済み)
  FROM `clas-analytics.lake.purchasing_detail` pd
  INNER JOIN `clas-analytics.lake.part_version` pv ON pv.id = pd.part_version_id
  WHERE pd.deleted_at IS NULL
    AND pd.cost IS NOT NULL AND pd.cost > 0
  GROUP BY pv.part_id
),
part_avg_cost AS (
  SELECT
    COALESCE(sc.part_id, pc.part_id) AS part_id,
    COALESCE(sc.avg_cost, pc.avg_cost) AS avg_cost,
    CASE WHEN sc.avg_cost IS NOT NULL THEN '在庫cost平均' ELSE '発注cost平均' END AS cost_source
  FROM part_stock_cost sc
  FULL OUTER JOIN part_po_cost pc ON pc.part_id = sc.part_id
),
-- SKU構成の全パーツについて、目標セット数に対する不足数を計算
partner_needs AS (
  SELECT
    ts.logical_sku_hash,
    ts.business_area,
    ts.target_sets,
    spd.part_id AS partner_part_id,
    spd.quantity AS required_qty,
    COALESCE(l.leftover_available, 0) AS partner_leftover,
    GREATEST(spd.quantity * ts.target_sets - COALESCE(l.leftover_available, 0), 0) AS shortage_qty
  FROM sku_target_sets ts
  INNER JOIN sku_part_detail spd ON spd.logical_sku_hash = ts.logical_sku_hash
  LEFT JOIN leftover_per_sku_part l
    ON l.logical_sku_hash = ts.logical_sku_hash
    AND l.business_area = ts.business_area
    AND l.part_id = spd.part_id
  WHERE ts.target_sets > 0
),
-- 相方パーツの管理対象外(Impossibility)在庫を分類タグ別にカウント (全エリア)
partner_impossible AS (
  SELECT
    part_id,
    STRING_AGG(CONCAT(cls_ja, ': ', CAST(cnt AS STRING)), '\n' ORDER BY cnt DESC, cls_ja) AS impossible_desc
  FROM (
    SELECT
      part_id,
      CASE classification_of_impossibility
        WHEN 'WarehouseLost' THEN '庫内紛失／棚卸差異'
        WHEN 'CustomerLost' THEN '顧客・配送業者による紛失'
        WHEN 'PlannedDisposal' THEN '計画廃棄'
        WHEN 'SoldToRecycler' THEN '売却（処分）'
        WHEN 'SoldToCustomer' THEN '売却（顧客）'
        WHEN 'SoldToBusiness' THEN '売却（法人案件）'
        WHEN 'SoldToEcommerce' THEN '売却（EC）'
        WHEN 'SoldToExternalSale' THEN '売却（外部販売）'
        WHEN 'SoldOn30days' THEN '売却（30日購入）'
        WHEN 'SampleDisposal' THEN 'サンプルの廃棄'
        WHEN 'InitialDefect' THEN '初期不良'
        WHEN 'InspectionUnnecessaryDisposal' THEN '検品不要廃棄'
        WHEN 'Malfunction' THEN '動作不良'
        WHEN 'ProductDefectScratch' THEN '商品不良（キズ）'
        WHEN 'ProductDefectDent' THEN '商品不良（へこみ）'
        WHEN 'ProductDefectDirt' THEN '商品不良（汚れ）'
        WHEN 'ProductDefectMold' THEN '商品不良（カビ）'
        WHEN 'ProductDefectDamage' THEN '商品不良（破損）'
        WHEN 'ProductDefectSmell' THEN '商品不良（におい）'
        WHEN 'PartProcurementNotPossible' THEN '部品・消耗品調達不可'
        WHEN 'PartUnification' THEN 'パーツ統合'
        WHEN 'BadDebt' THEN '貸倒'
        WHEN 'MarketingUsage' THEN 'マーケティング利用'
        WHEN 'EquipmentUsage' THEN '備品利用'
        ELSE COALESCE(classification_of_impossibility, '未分類')
      END AS cls_ja,
      COUNT(*) AS cnt
    FROM `clas-analytics.lake.stock`
    WHERE deleted_at IS NULL AND status = 'Impossibility'
    GROUP BY part_id, cls_ja
  )
  GROUP BY part_id
)
-- ============================================================================
-- 最終出力: 1行 = SKU x エリア x 不足相方パーツ
-- ============================================================================
SELECT
  si.image_url AS `画像`,
  swp.representative_sku_id AS `SKU_ID`,
  swp.product_id AS `商品ID`,
  CONCAT('https://clas.style/admin/product/', CAST(swp.product_id AS STRING), '#images') AS `商品IDリンク`,
  si.series_name AS `シリーズ名`,
  COALESCE(si.product_name, prod.name) AS `商品名`,
  si.attribute_value AS `属性`,
  si.category_name AS `カテゴリ`,
  si.supplier AS `サプライヤー`,
  CASE pn.business_area WHEN 'Kanto' THEN '関東' WHEN 'Kansai' THEN '関西' WHEN 'Kyushu' THEN '九州' END AS `エリア`,
  sls.leftover_parts_desc AS `余りパーツ内訳`,
  sls.leftover_available_total AS `余り点数_貸出可能`,
  pn.target_sets AS `追加で組める商品数`,
  pn.partner_part_id AS `相方パーツID`,
  pp.name AS `相方パーツ名`,
  pn.required_qty AS `必要点数_1商品あたり`,
  pn.partner_leftover AS `相方の余り在庫`,
  pn.shortage_qty AS `追加必要点数`,
  pac.avg_cost AS `単価`,
  pn.shortage_qty * pac.avg_cost AS `必要金額`,
  pi.impossible_desc AS `相方の管理対象外内訳`,
  -- 以下はシート(詳細版)には出さない (GAS側で除外)。SKU単位シート・並び順・デバッグ用に保持
  sls.leftover_ids AS `余り在庫ID`,
  spp.prep_ids AS `配送準備中案件ID`,
  spp.prep_names AS `配送準備中案件名`,
  spp.prep_links AS `配送準備中案件リンク`,
  pac.cost_source AS `単価出所`,
  SUM(pn.shortage_qty * pac.avg_cost) OVER (
    PARTITION BY pn.logical_sku_hash, pn.business_area
  ) AS `SKU合計必要金額`
FROM partner_needs pn
INNER JOIN sku_with_priority swp ON swp.logical_sku_hash = pn.logical_sku_hash
INNER JOIN `clas-analytics.lake.product` prod ON prod.id = swp.product_id
INNER JOIN `clas-analytics.lake.part` pp ON pp.id = pn.partner_part_id
INNER JOIN sku_leftover_summary sls
  ON sls.logical_sku_hash = pn.logical_sku_hash AND sls.business_area = pn.business_area
LEFT JOIN sku_info si ON si.logical_sku_hash = pn.logical_sku_hash
LEFT JOIN part_avg_cost pac ON pac.part_id = pn.partner_part_id
LEFT JOIN partner_impossible pi ON pi.part_id = pn.partner_part_id
LEFT JOIN sku_preparing_props spp ON spp.logical_sku_hash = pn.logical_sku_hash
WHERE pn.shortage_qty > 0
;

-- 確認用サマリ
SELECT `エリア`, COUNT(*) AS row_count, SUM(`必要金額`) AS total_amount
FROM `clas-analytics.sandbox.leftover_reason_report`
GROUP BY `エリア`;
