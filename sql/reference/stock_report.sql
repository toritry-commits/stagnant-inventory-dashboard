WITH 
lent_info AS (
  SELECT
    stock_id,
    MAX(lent.id) AS id,
    MAX(category) AS category,
    MIN(shipped_at) AS first_shipped_at,
    MAX(shipped_at) AS latest_shipped_at,
    MAX(return_at) AS latest_return_at
  FROM `lake.lent` AS lent
  INNER JOIN `lake.lent_detail` AS lent_detail
  ON lent.id = lent_detail.lent_id
  AND lent.status IN ("Adjusting", "Preparing", "Lending", "AdjustingReturn", "PreparingReturn", "Returned") 
  AND lent.deleted_at IS NULL
  AND lent_detail.deleted_at IS NULL
  GROUP BY stock_id
),
to_b_info AS (
  SELECT
    stock_id,
    MAX(lent_to_b.id) AS id,
    MIN(start_at) AS first_shipped_at,
    MAX(start_at) AS latest_shipped_at,
    MAX(end_at) AS latest_return_at
  FROM `lake.lent_to_b_detail` As lent_to_b_detail
  INNER JOIN `lake.stock` AS stock
  ON lent_to_b_detail.stock_id = stock.id
  INNER JOIN `lake.lent_to_b` AS lent_to_b
  ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
  AND lent_to_b_detail.deleted_at IS NULL
  AND lent_to_b.`status` IN ("Preparing", "InUse", "Ended", "Sold")
  GROUP BY stock_id
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
part_attribute AS (
SELECT
  part.id AS part_id,
  MAX(CASE 
    WHEN product_customer.customer = 'Consumer' THEN '一般顧客'
    WHEN product_customer.customer = 'BusinessSmallOffice' THEN '法人'
    ELSE 'unknown' END) AS customer
FROM
  `lake.part` AS part
LEFT JOIN `lake.attribute_value_part` AS attribute_value_part 
  ON attribute_value_part.part_id = part.id AND attribute_value_part.deleted_at IS NULL
LEFT JOIN `lake.attribute_value` AS attribute_value 
  ON attribute_value.id = attribute_value_part.attribute_value_id AND attribute_value.deleted_at IS NULL
LEFT JOIN `lake.attribute` AS attribute 
  ON attribute.id = attribute_value.attribute_id AND attribute.deleted_at IS NULL
LEFT JOIN `lake.product` AS product 
  ON product.id = attribute.product_id AND product.deleted_at IS NULL
LEFT JOIN `lake.product_customer` AS product_customer 
  ON product.id = product_customer.product_id AND product_customer.deleted_at IS NULL
WHERE
  part.deleted_at IS NULL
GROUP BY
  1
),
part_category AS(
  SELECT
    p.id part_id, pc.category1, pc.category2, pc.category3
  FROM
    `lake.part` p
  INNER JOIN (
    SELECT
      attribute_value_id, part_id,
      row_number() over (partition by part_id order by created_at) as created_order
    FROM
      `lake.attribute_value_part`
    WHERE
      deleted_at is NULL
    QUALIFY
      created_order = 1
  )  att_vp ON p.id = att_vp.part_id
  INNER JOIN (
    SELECT
      id, attribute_id,
      row_number() over (partition by id order by created_at) as created_order
    FROM
      `lake.attribute_value`
    WHERE
      deleted_at is NULL
    QUALIFY
      created_order = 1
  ) att_v ON att_v.id = att_vp.attribute_value_id
  INNER JOIN (
    SELECT
      id, product_id,
      row_number() over (partition by id order by created_at) as created_order
    FROM
      `lake.attribute`
    WHERE
      deleted_at is NULL
    QUALIFY
      created_order = 1
  ) att ON att_v.attribute_id = att.id
  INNER JOIN (
    SELECT
      id, series_id,
      row_number() over (partition by id order by created_at) as created_order
    FROM
      `lake.product`
    WHERE
      deleted_at is NULL
    QUALIFY
      created_order = 1
  ) pro ON att.product_id = pro.id
  INNER JOIN
    `warehouse.product_category` pc ON pro.id = pc.product_id
  WHERE
    p.deleted_at is NULL
    AND NOT REGEXP_CONTAINS(p.name, "^【法人】")
),
recover_status AS (
  SELECT
    stock_id,
    CASE SUM(assigned)
      WHEN 0 THEN "割当なし"
      WHEN 1 THEN "C向け注文割当中"
      WHEN 2 THEN "B向け注文割当中"
      ELSE "データ不整合の可能性あり"
    END AS assigned
  FROM (
    SELECT
      stock_id,
      IF(lc.status not in ("Returned", "PurchaseFailed", "Cancel"), 1, 0) as assigned
    FROM
      lent_info
    LEFT JOIN `lake.lent` lc
    USING(id)
    UNION ALL
    SELECT
      stock_id,
      IF(lb.status in ("Preparing", "InUse"), 2, 0) as assigned
    FROM
      to_b_info
    LEFT JOIN `lake.lent_to_b` lb
    USING(id)
  ) lending_info
  GROUP BY stock_id
),
external_sale_stock AS (
  SELECT
    stock_id,
    status AS sale_status
  FROM
    `lake.external_sale_stock`
  where deleted_at is null
),
base AS (
  SELECT
    stock.id AS stock_id,
    CONCAT('https://clas.style/admin/purchasing/',stock.id) AS purchasing_url,
    CONCAT('https://clas.style/admin/stock/',stock.id) AS stock_url,
    supplier.name AS supplier,
    CONCAT('https://clas.style/admin/part/', stock.part_id) AS part_url,
    part.name AS part_name,
    part_version.version AS part_version,
    part_version.packing_size AS packing_size,
    part.id AS part_id,
    `code`,
    cost,
    arrival_at,
    `status`,
    (CASE `status`
      WHEN "Waiting" THEN "入荷待ち"
      WHEN "Ready" THEN "貸出可能"
      WHEN "Lending" THEN "貸出中"
      WHEN "Recovery" THEN "リカバリー中"
      WHEN "Impossibility" THEN "管理対象外"
      WHEN "LendingToB" THEN "B向け貸出中"
      WHEN "Ordered" THEN "MD発注済"
      WHEN "NotDemanded" THEN "架空在庫"
      WHEN "NotOrdered" THEN "MD未発注"
      ELSE `status`
    END) AS status_ja,
    recover_status.assigned,
    external_sale_stock.sale_status AS sale_status,
    CASE
      WHEN lent_info.first_shipped_at IS NOT NULL AND to_b_info.first_shipped_at IS NOT NULL
        THEN LEAST(lent_info.first_shipped_at, to_b_info.first_shipped_at)
      ELSE COALESCE(lent_info.first_shipped_at, to_b_info.first_shipped_at, NULL)
    END AS first_shipped_at,
    CASE
      WHEN lent_info.latest_shipped_at IS NOT NULL AND to_b_info.latest_shipped_at IS NOT NULL
        THEN GREATEST(lent_info.latest_shipped_at, to_b_info.latest_shipped_at)
      ELSE COALESCE(lent_info.latest_shipped_at, to_b_info.latest_shipped_at, NULL)
    END AS latest_shipped_at,
    CASE
      WHEN lent_info.latest_return_at IS NOT NULL AND to_b_info.latest_return_at IS NOT NULL
        THEN GREATEST(lent_info.latest_return_at, to_b_info.latest_return_at)
      ELSE COALESCE(lent_info.latest_return_at, to_b_info.latest_return_at, NULL)
    END AS latest_return_at,
    LAST_DAY(DATE_ADD(CURRENT_DATE("Asia/Tokyo"), INTERVAL -1 MONTH)) AS calc_base_date,
    TRUNC(SAFE_DIVIDE(cost, (part.depreciation_period * 12)) + 0.999, 0) AS monthly_depreciation_amount,
    CASE
      WHEN CAST(FORMAT_DATE("%m", CURRENT_DATE("Asia/Tokyo")) AS INT64) < 3 THEN
        LAST_DAY(DATE(FORMAT_DATE("%Y-02-01", DATE_ADD(CURRENT_DATE("Asia/Tokyo"), INTERVAL -1 YEAR))))
      ELSE
        LAST_DAY(DATE(FORMAT_DATE("%Y-02-01", CURRENT_DATE("Asia/Tokyo"))))
    END AS pre_end_term,
    CASE
      WHEN CAST(FORMAT_DATE("%m", CURRENT_DATE("Asia/Tokyo")) AS INT64) < 3 THEN
        FORMAT_DATE("%Y-03-01", DATE_ADD(CURRENT_DATE("Asia/Tokyo"), INTERVAL -1 YEAR))
      ELSE
        FORMAT_DATE("%Y-03-01", CURRENT_DATE("Asia/Tokyo"))
    END AS beginning_term,
    CASE
      WHEN CAST(FORMAT_DATE("%m", CURRENT_DATE("Asia/Tokyo")) AS INT64) < 3 THEN
        LAST_DAY(DATE(FORMAT_DATE("%Y-02-01", CURRENT_DATE("Asia/Tokyo"))))
      ELSE
        LAST_DAY(DATE(FORMAT_DATE("%Y-02-01", DATE_ADD(CURRENT_DATE("Asia/Tokyo"), INTERVAL 1 YEAR))))
    END AS end_term,
    warehouse.name AS warehouse_name,
    location.name AS location_name,
    _rank_,
    stock.updated_at AS stock_updated_at,
    stock.impairment_date AS impairment_date,
    CASE part.inspection_priority
      WHEN 'Highest' THEN "優先度1:最高(優先検品タグ)"
      WHEN 'High' THEN "優先度2:高(レベシェア商品)"
      WHEN 'Medium' THEN "優先度3:中(売上貢献度上位)"
      WHEN 'Low' THEN "優先度4:低(検品待ち多数)"
      WHEN 'Lowest' THEN "優先度5:最低(優先度1〜4以外)"
      WHEN 'NoNeed' THEN  "検品対象外(廃棄対象)"
      ELSE '優先度無' END inspection_priority,
    purchasing_info.purchasing_number,
    purchasing_info.purchasing_order_date,
    CASE
      WHEN INSTR(purchasing_info.purchasing_note, "【社内稟議番号：") > 0
      THEN SPLIT(SPLIT(purchasing_info.purchasing_note, "【社内稟議番号：")[OFFSET(1)], "】")[OFFSET(0)]
      ELSE ""
    END AS approval_number,
    CASE 
      WHEN purchasing_info.purchasing_id IS NOT NULL
      THEN CONCAT("https://clas.style/purchasing_order/",purchasing_info.purchasing_id,".html?preview=false")
      ELSE "発注書リンクなし"
    END AS purchase_order_link,
    part_attribute.customer,
    purchasing_info.orderer_email as orderer_email,
    part_category.category1,
    part_category.category2
  FROM `lake.stock` stock
  INNER JOIN `lake.part` AS part
  ON stock.part_id = part.id AND part.deleted_at IS NULL
  LEFT JOIN `lake.location` location
  ON location.id = stock.location_id
  LEFT JOIN `lake.warehouse` warehouse
  ON warehouse.id = location.warehouse_id
  INNER JOIN `lake.supplier` supplier
  ON supplier.id = stock.supplier_id
  LEFT JOIN `lake.part_version` part_version
  ON part_version.id = stock.part_version_id AND part_version.deleted_at IS NULL
  LEFT JOIN lent_info
  ON lent_info.stock_id = stock.id
  LEFT JOIN to_b_info
  ON to_b_info.stock_id = stock.id
  LEFT JOIN purchasing_info
  ON purchasing_info.stock_id = stock.id
  LEFT JOIN part_attribute
  ON stock.part_id = part_attribute.part_id
  LEFT JOIN recover_status
  ON recover_status.stock_id = stock.id
  LEFT JOIN external_sale_stock
  ON external_sale_stock.stock_id = stock.id
  LEFT JOIN part_category
  ON part_category.part_id = stock.part_id
  WHERE stock.deleted_at is null
  AND NOT stock.sample
)
SELECT
  stock_id,
  stock_url,
  supplier,
  part_id,
  part_url,
  part_name,
  part_version,
  packing_size,
  `code`,
  cost,
  `status`,
  status_ja,
  assigned,
  sale_status,
  _rank_,
  warehouse_name,
  location_name,
  arrival_at,
  first_shipped_at,
  latest_shipped_at,
  latest_return_at,
  monthly_depreciation_amount,
  beginning_term,
  CASE WHEN (cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(beginning_term), DATE(first_shipped_at), MONTH) + 1, 0)) < 0
    THEN 0
    ELSE cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(beginning_term), DATE(first_shipped_at), MONTH) + 1, 0)
    END AS beginning_book_value,
  calc_base_date,
  CASE WHEN (cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(calc_base_date), DATE(first_shipped_at), MONTH) + 1, 0)) < 0
    THEN 0
    ELSE cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(calc_base_date), DATE(first_shipped_at), MONTH) + 1, 0)
    END AS month_end_book_value,
  end_term,
  CASE WHEN (cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(end_term), DATE(first_shipped_at), MONTH) + 1, 0)) < 0
    THEN 0
    ELSE cost - monthly_depreciation_amount * COALESCE(DATE_DIFF(DATE(end_term), DATE(first_shipped_at), MONTH) + 1, 0)
    END AS end_book_value,
  stock_updated_at,
  inspection_priority,
  impairment_date,
  purchasing_number,
  purchasing_order_date,
  approval_number,
  purchase_order_link,
  customer,
  orderer_email,
  CASE
    WHEN orderer_email is null THEN "不明"
    WHEN orderer_email LIKE "%info+c-order%" THEN "EC"
    ELSE "法人"
  END AS orderer_attribute,
  IFNULL(pv_supplier.name, "なし") main_supplier,
  category1,
  category2
FROM base
LEFT JOIN (
  SELECT
    pv.part_id, sp.name
  FROM
    `lake.part_version` pv
  INNER JOIN
    `lake.supplier` sp ON sp.id = pv.supplier_id
  QUALIFY
    row_number() over (partition by part_id order by version desc) = 1
) pv_supplier USING(part_id)
