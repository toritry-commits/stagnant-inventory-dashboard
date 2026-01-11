WITH to_b_name AS (
  SELECT
    STRING_AGG(proposition.name) AS proposition_name,
    stock_id,
    lent_to_b.lease_start_at
  FROM
    `lake.proposition` proposition
  INNER JOIN `lake.contract` AS contract ON proposition.id = contract.proposition_id
  INNER JOIN `lake.contract_destination` AS contract_destination ON contract.id = contract_destination.contract_id
  INNER JOIN `lake.lent_to_b` AS lent_to_b ON lent_to_b.contract_destination_id = contract_destination.id AND lent_to_b.deleted_at is NULL
  INNER JOIN `lake.lent_to_b_detail` lent_to_b_detail ON lent_to_b_detail.lent_to_b_id = lent_to_b.id AND lent_to_b_detail.deleted_at is NULL
  GROUP BY stock_id, lent_to_b.lease_start_at
),
purchasing_info AS (
  SELECT
    p.id as purchasing_id
    , p.purchasing_number as purchasing_number 
    , p.order_date as purchasing_order_date
    , p.note as purchasing_note
    , pds.stock_id as stock_id
    , p.orderer_email as orderer_email
  FROM (SELECT id, `status`, purchasing_number, order_date, note, orderer_email FROM `lake.purchasing` WHERE deleted_at IS NULL) AS p
  INNER JOIN (SELECT purchasing_id, id FROM `lake.purchasing_detail` WHERE deleted_at IS NULL) AS pd
  ON pd.purchasing_id = p.id
  INNER JOIN (SELECT purchasing_detail_id, stock_id FROM `lake.purchasing_detail_stock` WHERE deleted_at IS NULL) AS pds 
  ON pds.purchasing_detail_id = pd.id
  WHERE p.`status` = "Done"
),
stock_category AS (
  select
   sr.stock_id as stock_id,
   sr.category2 as category
  from
  `mart.stock_report` sr
),

external_info AS (
  SELECT
    ess.stock_id as stock_id
    ,esp.created_at as esp_created_at
    ,esp.authorized_at as authorized_at
    ,esp.id as esp_id
    ,esp.status as esp_status
  FROM
    `lake.external_sale_stock` ess
  INNER JOIN `lake.external_sale_product` esp
  ON ess.external_sale_product_id = esp.id and esp.deleted_at is null
  where ess.deleted_at is null
),

stock_cost AS (
  SELECT
    sac.stock_id as stock_id,
    sac.overhead_cost as overhead_cost,
    sac.discount as discount
  FROM
    `lake.stock_acquisition_costs` sac
    where sac.deleted_at is null
),

external_sale_stock AS (
  SELECT
    stock_id,
    status AS sale_status
  FROM
    `lake.external_sale_stock`
  where deleted_at is null
),

base as (
  SELECT
    LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS calc_base_date,
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) < 3 THEN
        LAST_DAY(DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 2, 1))
      ELSE
        LAST_DAY(DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 1))
    END AS pre_end_term,
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) < 3 THEN
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 3, 1)
      ELSE
        DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 3, 1)
    END AS beginning_term,
    CASE
      WHEN EXTRACT(MONTH FROM CURRENT_DATE()) < 3 THEN
        LAST_DAY(DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 2, 1))
      ELSE
        LAST_DAY(DATE(EXTRACT(YEAR FROM CURRENT_DATE()) + 1, 2, 1))
    END AS end_term,
    LAST_DAY(DATE(
      EXTRACT(YEAR FROM CURRENT_DATE("Asia/Tokyo")) + IF(EXTRACT(MONTH FROM CURRENT_DATE("Asia/Tokyo")) = 12, 1, 0), -- 12月の場合は翌年にする
      CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE("Asia/Tokyo")) IN (12, 1, 2) THEN 2
        WHEN EXTRACT(MONTH FROM CURRENT_DATE("Asia/Tokyo")) IN (3, 4, 5) THEN 5
        WHEN EXTRACT(MONTH FROM CURRENT_DATE("Asia/Tokyo")) IN (6, 7, 8) THEN 8
        ELSE 11 -- 9, 10, 11月の場合
      END,
      1
    )) AS next_term_end_date,
    stock.id  AS stock_id,
    part.id AS part_id,
    part.name AS part_name,
    part.depreciation_period AS depreciation_period,
    supplier.name AS supplier_name,
    stock.cost,
    CASE
      WHEN supplier.name = "株式会社カンム 契約No.20220005" THEN DATE "2022-5-1"
      WHEN supplier.name = "株式会社カンム 契約No.20220006" THEN DATE "2022-6-1"
      WHEN lent_info.first_shipped_at IS NOT NULL AND to_b_info.first_shipped_at IS NOT NULL THEN LEAST(lent_info.first_shipped_at, to_b_info.first_shipped_at)
      ELSE COALESCE(lent_info.first_shipped_at, to_b_info.first_shipped_at, null)
    END AS first_shipped_at,
    CASE
     WHEN lent_info.last_shipped_at IS NOT NULL
          AND to_b_info.last_shipped_at IS NOT NULL THEN GREATEST(lent_info.last_shipped_at, to_b_info.last_shipped_at)
     ELSE COALESCE(lent_info.last_shipped_at, to_b_info.last_shipped_at, null)
    END AS last_shipped_at,
    CASE
        WHEN lent_info.last_return_at IS NOT NULL
            AND to_b_info.last_return_at IS NOT NULL THEN GREATEST(lent_info.last_return_at, to_b_info.last_return_at)
        ELSE COALESCE(lent_info.last_return_at, to_b_info.last_return_at)
    END AS last_return_at,
    IF(part.depreciation_period = 0, 0, TRUNC(cost / (part.depreciation_period * 12) + 0.999, 0)) AS monthly_depreciation_amount,
    inspected_at,
    CASE
      WHEN stock.status = "Impossibility" THEN stock.impossibled_at
      ELSE null
    END AS impossibled_at,
    stock.classification_of_impossibility AS classification_of_impossibility_raw,
    CASE stock.classification_of_impossibility
      WHEN "WarehouseLost" THEN "庫内紛失／棚卸差異"
      WHEN "CustomerLost" THEN "顧客・配送業者による紛失"
      WHEN "PlannedDisposal" THEN "計画廃棄"
      WHEN "SoldToRecycler" THEN "売却（処分）"
      WHEN "SoldToCustomer" THEN "売却（顧客）"
      WHEN "SoldToBusiness" THEN "売却（法人案件）"
      WHEN "SoldToEcommerce" THEN "売却（EC）"
      WHEN "SampleDisposal" THEN "サンプルの廃棄"
      WHEN "InitialDefect" THEN "初期不良"
      WHEN "InspectionUnnecessaryDisposal" THEN "検品不要廃棄"
      WHEN "Malfunction" THEN "動作不良"
      WHEN "ProductDefectScratch" THEN "商品不良（キズ）"
      WHEN "ProductDefectDent" THEN "商品不良（へこみ）"
      WHEN "ProductDefectDirt" THEN "商品不良（汚れ）"
      WHEN "ProductDefectMold" THEN "商品不良（カビ）"
      WHEN "ProductDefectDamage" THEN "商品不良（破損）"
      WHEN "ProductDefectSmell" THEN "商品不良（におい）"
      WHEN "PartProcurementNotPossible" THEN "部品・消耗品調達不可"
      WHEN "PartUnification" THEN "パーツ統合"
      WHEN "BadDebt" THEN "貸倒"
      ELSE classification_of_impossibility
    END classification_of_impossibility,
    to_b_lease.first_shipped_at AS lease_first_shipped_at,
    bname_l.proposition_name AS lease_proposition_name,
    last_proposition_id,
    last_proposition_name,
    client_name,
    to_b_sold.sold_proposition_name AS sold_proposition_name,
    stock.status,
    stock.sample,
    impairment_date,
    arrival_at,
    purchasing_info.purchasing_id,
    purchasing_info.purchasing_number,
    external_info.esp_created_at,
    external_info.authorized_at,
    external_info.esp_id,
    external_info.esp_status,
    stock_category.category as category,
    stock_cost.overhead_cost,
    stock_cost.discount,
    stock._rank_ AS _rank_, 
    warehouse.name AS warehouse_name,
    location.name AS location_name,
    external_sale_stock.sale_status AS sale_status
  FROM
    `lake.stock` stock
  LEFT JOIN external_sale_stock
    ON external_sale_stock.stock_id = stock.id  
  LEFT JOIN 
    `lake.location` location ON location.id = stock.location_id
  LEFT JOIN
    `lake.warehouse` warehouse ON warehouse.id = location.warehouse_id
  INNER JOIN
    `lake.part` part ON part.id = stock.part_id
  AND part.deleted_at IS NULL
  INNER JOIN `lake.supplier` supplier
  ON supplier.id  = stock.supplier_id
  LEFT JOIN ( -- C向け初回出荷日（月末日以前のデータのみ取得）
    SELECT
      stock_id,
      MIN(CASE 
          WHEN shipped_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN shipped_at 
          ELSE NULL 
      END) AS first_shipped_at,
      MAX(CASE 
          WHEN shipped_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN shipped_at 
          ELSE NULL 
      END) AS last_shipped_at,
      MAX(CASE 
          WHEN return_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN return_at 
          ELSE NULL 
      END) AS last_return_at
    FROM `lake.lent` lent
    INNER JOIN `lake.lent_detail` lent_detail
    ON lent.id = lent_detail.lent_id
    AND lent.status IN ("Adjusting", "Preparing", "Lending", "AdjustingReturn", "PreparingReturn", "Returned")
    AND lent.deleted_at IS NULL
    AND (lent.shipped_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
         OR lent.return_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))
    GROUP BY stock_id
  ) AS lent_info ON lent_info.stock_id = stock.id
  LEFT JOIN ( -- B向け初回出荷日（月末日以前のデータのみ取得）
    SELECT
      stock_id,
      MIN(CASE 
          WHEN start_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN start_at 
          ELSE NULL 
      END) AS first_shipped_at,
      MAX(CASE 
          WHEN start_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN start_at 
          ELSE NULL 
      END) AS last_shipped_at,
      MAX(CASE 
          WHEN end_at <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN end_at 
          ELSE NULL 
      END) AS last_return_at
    FROM `lake.lent_to_b_detail` lent_to_b_detail
    INNER JOIN `lake.lent_to_b` lent_to_b
    ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
    LEFT JOIN `lake.stock` stock
    ON lent_to_b_detail.stock_id = stock.id
    AND lent_to_b_detail.deleted_at IS NULL
    GROUP BY stock_id
  ) AS to_b_info ON to_b_info.stock_id = stock.id
  LEFT JOIN ( -- B向け初回出荷日（リース）
    SELECT
      stock_id,
      MIN(lease_start_at) AS first_shipped_at,
      MAX(end_at) AS last_return_at
    FROM `lake.lent_to_b_detail` lent_to_b_detail
    INNER JOIN `lake.lent_to_b` lent_to_b
    ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
    LEFT JOIN `lake.stock` stock
    ON lent_to_b_detail.stock_id = stock.id
    AND lent_to_b_detail.deleted_at IS NULL
    WHERE lent_to_b.lease_start_at IS NOT NULL
    GROUP BY stock_id
  ) AS to_b_lease ON to_b_lease.stock_id = stock.id
  LEFT JOIN to_b_name AS bname_l ON bname_l.stock_id = stock.id
  AND bname_l.lease_start_at = to_b_lease.first_shipped_at
  LEFT JOIN (
    SELECT
      stock_id,
      last_proposition_id,
      sold_at,
      last_proposition_name,
      sold_proposition_name,
      client_name
    FROM (
      SELECT -- 売却絞り込み
        stock_id,
        proposition.id AS last_proposition_id,
        lent_to_b.start_at,
        lent_to_b.end_at AS sold_at,
        proposition.name AS last_proposition_name,
        proposition.name AS sold_proposition_name,
        client.name client_name
      FROM `lake.lent_to_b_detail` lent_to_b_detail
      INNER JOIN `lake.lent_to_b` lent_to_b
        ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
        AND lent_to_b_detail.deleted_at IS NULL
        AND lease_start_at IS NULL
        AND lent_to_b.status = 'Sold'
      INNER JOIN `lake.contract_destination` AS contract_destination ON contract_destination.id = lent_to_b.contract_destination_id
      INNER JOIN `lake.contract` AS contract ON contract.id = contract_destination.contract_id
      INNER JOIN `lake.proposition` AS proposition ON proposition.id = contract.proposition_id
      INNER JOIN `lake.client` client ON client.id = proposition.client_id
      UNION ALL
      SELECT -- 売却以外絞り込み
        stock_id,
        proposition.id AS last_proposition_id,
        lent_to_b.start_at,
        DATETIME("") AS sold_at,
        proposition.name AS last_proposition_name,
        "" AS sold_proposition_name,
        client.name client_name
      FROM `lake.lent_to_b_detail` lent_to_b_detail
      INNER JOIN `lake.lent_to_b` lent_to_b
        ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
        AND lent_to_b_detail.deleted_at IS NULL
        AND lease_start_at IS NULL
        AND lent_to_b.status NOT IN ('Sold', 'Cancel')
      INNER JOIN `lake.contract_destination` AS contract_destination ON contract_destination.id = lent_to_b.contract_destination_id
      INNER JOIN `lake.contract` AS contract ON contract.id = contract_destination.contract_id
      INNER JOIN `lake.proposition` AS proposition ON proposition.id = contract.proposition_id
      INNER JOIN `lake.client` client ON client.id = proposition.client_id
    )
    QUALIFY RANK() OVER(PARTITION BY stock_id ORDER BY start_at DESC) = 1
  ) to_b_sold ON to_b_sold.stock_id = stock.id
  LEFT JOIN purchasing_info ON purchasing_info.stock_id = stock.id
  LEFT JOIN external_info ON external_info.stock_id = stock.id
  LEFT JOIN stock_category ON stock_category.stock_id = stock.id
  LEFT JOIN stock_cost ON stock_cost.stock_id = stock.id
  WHERE
    stock.deleted_at IS NULL
    AND stock.status IN ("Ready","Lending","Recovery","Impossibility", "LendingToB", "Waiting", "Ordered")
)
SELECT
  calc_base_date,
  pre_end_term,
  beginning_term,
  end_term,
  stock_id,
  part_id,
  part_name,
  depreciation_period,
  supplier_name,
  status,
  cost,
  first_shipped_at,
  last_shipped_at,
  last_return_at,
  -- 未稼働日数（修正版）
  GREATEST(
    DATE_DIFF(
      calc_base_date,
      CASE
        WHEN (SELECT MAX(d) FROM UNNEST([arrival_at, inspected_at, last_shipped_at, last_return_at]) AS d) = last_shipped_at
             OR impossibled_at IS NOT NULL
             OR (classification_of_impossibility_raw IS NOT NULL AND classification_of_impossibility_raw != "")
        THEN calc_base_date
        ELSE (SELECT MAX(d) FROM UNNEST([arrival_at, last_shipped_at, last_return_at]) AS d WHERE d IS NOT NULL)
      END,
      DAY
    ),
    0
  ) AS idle_days,
    GREATEST(
    DATE_DIFF(
      CURRENT_DATETIME("Asia/Tokyo"),
      CASE
        WHEN (SELECT MAX(d) FROM UNNEST([arrival_at, inspected_at, last_shipped_at, last_return_at]) AS d) = last_shipped_at
             OR impossibled_at IS NOT NULL
             OR (classification_of_impossibility_raw IS NOT NULL AND classification_of_impossibility_raw != "")
        THEN calc_base_date
        ELSE (SELECT MAX(d) FROM UNNEST([arrival_at, last_shipped_at, last_return_at]) AS d WHERE d IS NOT NULL)
      END,
      DAY
    ),
    0
  ) AS idle_days_today,
    GREATEST(
    DATE_DIFF(
      next_term_end_date,
      CASE
        WHEN (SELECT MAX(d) FROM UNNEST([arrival_at, inspected_at, last_shipped_at, last_return_at]) AS d) = last_shipped_at
             OR impossibled_at IS NOT NULL
             OR (classification_of_impossibility_raw IS NOT NULL AND classification_of_impossibility_raw != "")
        THEN calc_base_date
        ELSE (SELECT MAX(d) FROM UNNEST([arrival_at, last_shipped_at, last_return_at]) AS d WHERE d IS NOT NULL)
      END,
      DAY
    ),
    0
  ) AS idle_days_end_term,
    GREATEST(
    DATE_DIFF(
      LAST_DAY(DATE_ADD(next_term_end_date, INTERVAL 3 MONTH)), -- 次期末に3ヶ月足して次の期末にする
      CASE
        WHEN (SELECT MAX(d) FROM UNNEST([arrival_at, inspected_at, last_shipped_at, last_return_at]) AS d) = last_shipped_at
             OR impossibled_at IS NOT NULL
             OR (classification_of_impossibility_raw IS NOT NULL AND classification_of_impossibility_raw != "")
        THEN calc_base_date
        ELSE (SELECT MAX(d) FROM UNNEST([arrival_at, last_shipped_at, last_return_at]) AS d WHERE d IS NOT NULL)
      END,
      DAY
    ),
    0
  ) AS idle_days_next_end_term,
  monthly_depreciation_amount,
  inspected_at,
  impossibled_at,
  classification_of_impossibility,
  lease_first_shipped_at,
  lease_proposition_name,
  last_proposition_id,
  last_proposition_name,
  sold_proposition_name,
  client_name,
  sample,
  impairment_date,
  CASE
	WHEN first_shipped_at > pre_end_term or first_shipped_at is NULL THEN 0
    WHEN impairment_date <= pre_end_term AND ((impossibled_at is NULL AND lease_first_shipped_at is NULL) OR impossibled_at > impairment_date OR lease_first_shipped_at > impairment_date)
    	THEN GREATEST(DATE_DIFF(impairment_date, first_shipped_at, MONTH)+ 1, 0)
    WHEN lease_first_shipped_at <= pre_end_term
    	THEN GREATEST(DATE_DIFF(lease_first_shipped_at, first_shipped_at, MONTH), 0)
    WHEN impossibled_at <= pre_end_term
    	THEN GREATEST(DATE_DIFF(impossibled_at, first_shipped_at, MONTH), 0)
    ELSE DATE_DIFF(pre_end_term, first_shipped_at, MONTH) + 1
  END beta,
  arrival_at,
  purchasing_id,
  purchasing_number,
  esp_created_at,
  authorized_at,
  esp_id,
  esp_status,
  category,
  overhead_cost,
  discount,
  _rank_,
  warehouse_name,
  location_name,
  sale_status
FROM
  base

