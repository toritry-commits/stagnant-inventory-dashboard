-- ============================================================================
-- エリア別「余りパーツ」在庫ID集計  (mart非依存・自己完結版 / 推奨)
-- ============================================================================
-- 目的 : sp_sku_inventory_v2 が作る mart.sku_inventory_v2 の
--        `関東_余りids` / `関西_余りids` / `九州_余りids` を、
--        mart に依存せず lake.* だけで再計算する。
-- 出力 : area別の在庫ID総数 (id_count) と 余りを持つSKU数 (sku_with_leftover)
-- 構成 : 一部を TEMP テーブル化 + CTE のマルチステートメント・スクリプト
--        (単一WITHクエリは BigQuery のプランナー制限で不可)
-- 実行 : bq query --use_legacy_sql=false --location=asia-northeast1 --project_id=clas-analytics < このファイル
-- 注意 : lake.stock を日付ピンなしで直接読むため、実行タイミングで数値は変動する (ライブ値)
-- 由来 : order-quantity-dashboard/sql/mart/sp_sku_inventory_v2.sql から抽出
-- 検証 : SP丸写しの faithful 版と結果が完全一致することを確認済み
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
-- Final statement: remaining Section 1.5 / 12.5 CTEs + 3-row UNION ALL aggregate
-- ============================================================================
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
stock_ids_per_part AS (
  SELECT
    logical_sku_hash,
    business_area,
    part_id,
    status_label,
    is_assembled,
    COUNT(*) AS cnt,
    STRING_AGG(labeled_id, ',' ORDER BY stock_order) AS ids_str
  FROM stock_id_with_status
  GROUP BY logical_sku_hash, business_area, part_id, status_label, is_assembled
),
sku_stock_ids_by_area AS (
  SELECT
    logical_sku_hash,
    STRING_AGG(IF(business_area = 'Kanto' AND is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kanto_assembled_ids,
    STRING_AGG(IF(business_area = 'Kanto' AND NOT is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kanto_leftover_ids,
    STRING_AGG(IF(business_area = 'Kansai' AND is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kansai_assembled_ids,
    STRING_AGG(IF(business_area = 'Kansai' AND NOT is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kansai_leftover_ids,
    STRING_AGG(IF(business_area = 'Kyushu' AND is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kyushu_assembled_ids,
    STRING_AGG(IF(business_area = 'Kyushu' AND NOT is_assembled,
      CONCAT('[', CAST(part_id AS STRING), ':', status_label, ':', CAST(cnt AS STRING), '点]', ids_str),
      NULL),
      '\n' ORDER BY part_id, CASE status_label WHEN '貸出可能' THEN 1 WHEN 'リカバリー中' THEN 2 ELSE 3 END
    ) AS kyushu_leftover_ids
  FROM stock_ids_per_part
  GROUP BY logical_sku_hash
)

-- ============================================================================
-- Final aggregate: エリア別 在庫ID総数
-- ============================================================================
SELECT '関東' AS area,
  SUM((LENGTH(kanto_leftover_ids)-LENGTH(REPLACE(kanto_leftover_ids,',','')))+(LENGTH(kanto_leftover_ids)-LENGTH(REPLACE(kanto_leftover_ids,'[','')))) AS id_count,
  COUNTIF(kanto_leftover_ids IS NOT NULL) AS sku_with_leftover
FROM sku_stock_ids_by_area
UNION ALL SELECT '関西', SUM((LENGTH(kansai_leftover_ids)-LENGTH(REPLACE(kansai_leftover_ids,',','')))+(LENGTH(kansai_leftover_ids)-LENGTH(REPLACE(kansai_leftover_ids,'[','')))) , COUNTIF(kansai_leftover_ids IS NOT NULL) FROM sku_stock_ids_by_area
UNION ALL SELECT '九州', SUM((LENGTH(kyushu_leftover_ids)-LENGTH(REPLACE(kyushu_leftover_ids,',','')))+(LENGTH(kyushu_leftover_ids)-LENGTH(REPLACE(kyushu_leftover_ids,'[','')))) , COUNTIF(kyushu_leftover_ids IS NOT NULL) FROM sku_stock_ids_by_area;
