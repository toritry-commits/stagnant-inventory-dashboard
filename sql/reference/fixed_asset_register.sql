declare st,et date;
SET st = '2018-03-01';
SET et = cast(FORMAT_DATE('%Y-%m-01',current_date('Asia/Tokyo')) AS date);

BEGIN
create or replace temp table temp_stock_union AS (
WITH
latest_stock_histroy_id AS (
  SELECT
    MAX(id) AS latest_id
  FROM
    `lake.stock_history`
  WHERE
    deleted_at is null
),

stock_now AS (
select
  latest_id + row_number() OVER(ORDER BY id) AS id,
  NULL created_at,
  NULL updated_at,
  NULL deleted_at,
  'stock' AS from_table,
  id AS stock_id,
  created_at AS stock_created_at,
  updated_at AS stock_updated_at,
  deleted_at AS stock_deleted_at,
  CASE WHEN arrival_at > '2050-01-01' THEN CAST(created_at AS date) ELSE arrival_at END AS arrival_at,
  classification_of_impossibility,
  code,
  cost,
  impossibled_at,
  CAST(inspected_at AS date) AS inspected_at,
  location_id,
  order_date_to_supplier,
  order_number_to_supplier,
  orderer_id,
  part_id,
  part_version_id,
  _rank_,
  sample,
  status,
  supplier_id
FROM
  `lake.stock` AS stock
CROSS JOIN
  latest_stock_histroy_id
)

select
  id AS stock_history_id,
  created_at,
  updated_at,
  deleted_at,
  'stock_history' AS from_table,
  sh.stock_id,
  stock_created_at,
  stock_updated_at,
  stock_deleted_at,
  CASE WHEN arrival_at > '2050-01-01' THEN CAST(stock_created_at AS date) ELSE arrival_at END AS arrival_at,
  classification_of_impossibility,
  code,
  cost,
  impossibled_at,
  CAST(inspected_at AS date) AS inspected_at,
  location_id,
  order_date_to_supplier,
  order_number_to_supplier,
  orderer_id,
  part_id,
  part_version_id,
  _rank_,
  CASE 
    WHEN sample = 'false' THEN False 
    WHEN sample = 'true' THEN True 
    ELSE NULL END sample,
  status,
  supplier_id
FROM
  `lake.stock_history` AS sh
WHERE
  deleted_at is null

UNION ALL

select
  id AS stock_history_id,
  NULL created_at,
  NULL updated_at,
  NULL deleted_at,
  'stock' AS from_table,
  stock_id,
  stock_created_at,
  stock_updated_at,
  stock_deleted_at,
  arrival_at,
  classification_of_impossibility,
  code,
  cost,
  impossibled_at,
  inspected_at,
  location_id,
  order_date_to_supplier,
  order_number_to_supplier,
  orderer_id,
  part_id,
  part_version_id,
  _rank_,
  sample,
  status,
  supplier_id
FROM
  stock_now

);

create or replace temp table monthly_latest_stock AS (
WITH
calendar as (
SELECT
  date_add(d,interval -1 day) record_day
FROM
  UNNEST(GENERATE_DATE_ARRAY('2018-01-01', '2028-03-01', INTERVAL 1 MONTH)) AS d

),

temp_latest_stock AS (
select
  record_day,
  stock_id,
  MAX(CASE 
        WHEN record_day >= CAST(stock_updated_at AS date) THEN stock_history_id
        ELSE NULL END) AS stock_history_id
FROM
  temp_stock_union
CROSS JOIN
  calendar
WHERE
  record_day >= arrival_at
GROUP BY
  record_day,
  stock_id
),

temp_first_stock AS (
select
  record_day,
  stock_id,
  MIN(stock_history_id) AS stock_history_id
FROM
  temp_stock_union
CROSS JOIN
  calendar
WHERE
  record_day >= arrival_at
  
GROUP BY
  record_day,
  stock_id

),

temp_monthly_latest_stock_id AS (
SELECT
  fs.record_day,
  COALESCE(ls.stock_id,fs.stock_id) AS stock_id,
  COALESCE(ls.stock_history_id,fs.stock_history_id) AS stock_history_id
FROM
  temp_first_stock AS fs
LEFT JOIN
  temp_latest_stock AS ls 
  ON fs.record_day = ls.record_day AND fs.stock_id = ls.stock_id
)

SELECT
  mls.record_day,
  su.stock_history_id,
  su.created_at,
  su.updated_at,
  su.deleted_at,
  su.from_table,
  su.stock_id,
  su.stock_created_at,
  su.stock_updated_at,
  su.stock_deleted_at,
  su.arrival_at,
  su.classification_of_impossibility,
  su.code,
  s.cost,
  su.impossibled_at,
  su.inspected_at,
  su.location_id,
  su.order_date_to_supplier,
  su.order_number_to_supplier,
  su.orderer_id,
  su.part_id,
  su.part_version_id,
  su._rank_,
  su.sample,
  su.status,
  su.supplier_id
FROM
  temp_monthly_latest_stock_id AS mls
LEFT JOIN
  temp_stock_union AS su 
  ON mls.stock_history_id = su.stock_history_id AND mls.stock_id = su.stock_id 
LEFT JOIN
  `lake.stock` AS s
  ON su.stock_id = s.id

);

create or replace table `finance.fixed_asset_register` AS (
WITH
--第１０期分まで用意
--3期以前は会計月が異なるので注意
calendar as (
SELECT
  d,
  date_add(DATE_ADD(d,interval 1 MONTH),interval -1 day) record_day,
  --d AS record_day,
  CASE 
    WHEN d >= '2018-04-01' AND '2019-04-01' > d THEN 1 
    WHEN d >= '2019-04-01' AND '2020-04-01' > d THEN 2
    WHEN d >= '2020-04-01' AND '2021-03-01' > d THEN 3
    WHEN d >= '2021-03-01' AND '2022-03-01' > d THEN 4
    WHEN d >= '2022-03-01' AND '2023-03-01' > d THEN 5
    WHEN d >= '2023-03-01' AND '2024-03-01' > d THEN 6
    WHEN d >= '2024-03-01' AND '2025-03-01' > d THEN 7
    WHEN d >= '2025-03-01' AND '2026-03-01' > d THEN 8
    WHEN d >= '2026-03-01' AND '2027-03-01' > d THEN 9
    WHEN d >= '2027-03-01' AND '2028-03-01' > d THEN 10
    ELSE NULL END term,
  cast(CASE 
    WHEN d >= '2018-04-01' AND '2019-04-01' > d THEN NULL #1
    WHEN d >= '2019-04-01' AND '2020-04-01' > d THEN '2019-03-31' #2
    WHEN d >= '2020-04-01' AND '2021-03-01' > d THEN '2020-03-31' #3
    WHEN d >= '2021-03-01' AND '2022-03-01' > d THEN '2021-02-28' #4
    WHEN d >= '2022-03-01' AND '2023-03-01' > d THEN '2022-02-28' #5
    WHEN d >= '2023-03-01' AND '2024-03-01' > d THEN '2023-02-28' #6
    WHEN d >= '2024-03-01' AND '2025-03-01' > d THEN '2024-02-28' #7
    WHEN d >= '2025-03-01' AND '2026-03-01' > d THEN '2025-02-28' #8
    WHEN d >= '2026-03-01' AND '2027-03-01' > d THEN '2026-02-28' #9
    WHEN d >= '2027-03-01' AND '2028-03-01' > d THEN '2027-02-28' #10
    ELSE NULL END AS date) pre_end_term,
  cast(CASE 
    WHEN d >= '2018-04-01' AND '2019-04-01' > d THEN '2018-04-01' #1
    WHEN d >= '2019-04-01' AND '2020-04-01' > d THEN '2019-04-01' #2
    WHEN d >= '2020-04-01' AND '2021-03-01' > d THEN '2020-04-01' #3
    WHEN d >= '2021-03-01' AND '2022-03-01' > d THEN '2021-03-01' #4
    WHEN d >= '2022-03-01' AND '2023-03-01' > d THEN '2022-03-01' #5
    WHEN d >= '2023-03-01' AND '2024-03-01' > d THEN '2023-03-01' #6
    WHEN d >= '2024-03-01' AND '2025-03-01' > d THEN '2024-03-01' #7
    WHEN d >= '2025-03-01' AND '2026-03-01' > d THEN '2025-03-01' #8
    WHEN d >= '2026-03-01' AND '2027-03-01' > d THEN '2026-03-01' #9
    WHEN d >= '2027-03-01' AND '2028-03-01' > d THEN '2027-03-01' #10
    ELSE NULL END AS date) beginning_term,
  cast(CASE 
    WHEN d >= '2018-04-01' AND '2019-04-01' > d THEN '2019-03-31' #1
    WHEN d >= '2019-04-01' AND '2020-04-01' > d THEN '2020-03-01' #2
    WHEN d >= '2020-04-01' AND '2021-03-01' > d THEN '2021-02-28' #3
    WHEN d >= '2021-03-01' AND '2022-03-01' > d THEN '2022-02-28' #4
    WHEN d >= '2022-03-01' AND '2023-03-01' > d THEN '2023-02-28' #5
    WHEN d >= '2023-03-01' AND '2024-03-01' > d THEN '2024-02-28' #6
    WHEN d >= '2024-03-01' AND '2025-03-01' > d THEN '2025-02-28' #7
    WHEN d >= '2025-03-01' AND '2026-03-01' > d THEN '2026-02-28' #8
    WHEN d >= '2026-03-01' AND '2027-03-01' > d THEN '2027-02-28' #9
    WHEN d >= '2027-03-01' AND '2028-03-01' > d THEN '2028-02-28' #10
    ELSE NULL END AS date) end_term
FROM
  UNNEST(GENERATE_DATE_ARRAY(st, et, INTERVAL 1 MONTH)) AS d
ORDER BY
  d
),

part_attribute AS (
SELECT
  part.id AS part_id,
  part.name AS part_name,
  part.depreciation_period,
  MAX(series.category) AS c_category,  -- MAX を GROUP_CONCAT に変えると全カテゴリを取得します
  CASE
    WHEN regexp_contains(part.`name`,'SF_') THEN 'Sofa'
    WHEN regexp_contains(part.`name`,'CH_') THEN 'Chair'
    WHEN regexp_contains(part.`name`,'TB_') THEN 'Table'
    WHEN regexp_contains(part.`name`,'OF_' ) THEN 'OtherFurniture'
    WHEN regexp_contains(part.`name`,'SR_' ) THEN 'Storage'
    WHEN regexp_contains(part.`name`,'EL_' ) THEN 'Electronics'
    WHEN regexp_contains(part.`name`,'ES_' ) THEN 'OtherElectronics'
    WHEN regexp_contains(part.`name`,'LT_' ) THEN 'Lighting'
    WHEN regexp_contains(part.`name`,'AT_' ) THEN 'Art'
    WHEN regexp_contains(part.`name`,'BD_' ) THEN 'Bed'
    WHEN regexp_contains(part.`name`,'MT_' ) THEN 'Mattress'
    WHEN regexp_contains(part.`name`,'GR_' ) THEN 'InteriorGreen'
    WHEN regexp_contains(part.`name`,'GL_' ) THEN 'Glass'
    WHEN regexp_contains(part.`name`,'KB_' ) THEN 'Kids&Babies'
    WHEN regexp_contains(part.`name`,'cp_' ) THEN 'components'
    WHEN regexp_contains(part.`name`,'TS_' ) THEN 'TVboard(Stand)'
    WHEN regexp_contains(part.`name`,'RG_' ) THEN 'Rug'
    WHEN regexp_contains(part.`name`,'CT_' ) THEN 'Curtain'
    ELSE '不明'
  END AS b_category,
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
LEFT JOIN `lake.series` AS series 
  ON series.id = product.series_id AND series.deleted_at IS NULL
WHERE
  part.deleted_at IS NULL
GROUP BY
  1,2,3,5

),

to_b_name AS (
  SELECT
    stock_id,
    STRING_AGG(distinct cast(proposition.id AS string)) AS proposition_name,
    lent_to_b.start_at
  FROM
    `lake.proposition` AS proposition
  LEFT JOIN
    `lake.contract` AS contract ON proposition.id = contract.proposition_id
  LEFT JOIN
    `lake.contract_destination` AS contract_destination ON contract.id = contract_destination.contract_id
  INNER JOIN 
    `lake.lent_to_b` AS lent_to_b ON lent_to_b.contract_destination_id = contract_destination.id
  INNER JOIN 
    `lake.lent_to_b_detail` AS lent_to_b_detail ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
  GROUP BY
    stock_id,
    lent_to_b.start_at
  ),

-- C向け初回出荷日
lent_info AS 
  (SELECT
    stock_id,
    MIN(shipped_at) AS first_shipped_at,
    MAX(return_at) AS last_return_at
  FROM 
    `lake.lent` AS lent
  INNER JOIN 
    `lake.lent_detail` AS lent_detail
    ON lent.id = lent_detail.lent_id
    AND lent.status IN ('Adjusting', 'Preparing', 'Lending', 'AdjustingReturn', 'PreparingReturn', 'Returned') 
    AND lent.deleted_at IS NULL
  GROUP BY 
    stock_id
  ),

-- B向け初回出荷日
to_b_info AS 
  (
  SELECT
    stock_id,
    MIN(start_at) AS first_shipped_at,
    MAX(end_at) AS last_return_at,
    MIN(CASE WHEN status = 'Sold' THEN start_at ELSE NULL END) AS toB_sold_date,
    MIN(lease_start_at) AS lease_start_at
  FROM 
    `lake.lent_to_b_detail` AS lent_to_b_detail
  INNER JOIN
    `lake.lent_to_b` AS lent_to_b
    ON lent_to_b_detail.lent_to_b_id = lent_to_b.id
  WHERE
    lent_to_b.deleted_at IS NULL
    AND 
    lent_to_b_detail.deleted_at IS NULL
  GROUP BY 
    stock_id
  ),

/* 破損紛失処理日*/ 
impossibility AS (
SELECT
  stock_id,
  --updated_atは廃棄された後もデータメンテ等で上書きされることがあるのでupdated_atの利用はなるべく避けるべき
  MIN(stock_updated_at) AS impossibility_date
FROM
  `lake.stock_history`
WHERE
  status = 'Impossibility'
GROUP BY
  stock_id
),

base AS (
select
  term,
  stock.record_day ,--AS calc_base_date,
  calendar.pre_end_term,
  calendar.beginning_term,
  calendar.end_term,
  stock.stock_id,
  stock.part_id,
  part.part_name,
  part.depreciation_period AS depreciation_period,
  part.c_category,
  part.b_category,
  stock.supplier_id,
  supplier.name AS supplier_name,
  stock.cost,
  stock.status AS stock_status,
  stock.order_date_to_supplier,
  COALESCE(CAST(stock.inspected_at AS date),stock.arrival_at) AS arrival_at,
  CASE
    WHEN lent_info.first_shipped_at IS NOT NULL AND to_b_info.first_shipped_at IS NOT NULL
      THEN LEAST(lent_info.first_shipped_at, to_b_info.first_shipped_at)
    ELSE COALESCE(lent_info.first_shipped_at, to_b_info.first_shipped_at, NULL)
  END AS first_shipped_at,
  to_b_name.proposition_name,
  CASE 
    WHEN part.depreciation_period != 0
      THEN TRUNC(stock.cost / (part.depreciation_period * 12) + 0.999, 0) 
      ELSE 0 END AS monthly_depreciation_amount,

  /* 破損紛失処理日*/ --仕様確認
  --impossibility.impossibility_date ,
  CASE 
    WHEN stock.record_day <= CAST(impossibility.impossibility_date AS date) THEN NULL
    ELSE impossibility.impossibility_date END AS impossibility_date,
   /* 廃棄日 */
  --stock.impossibled_at AS sold_date,
  CASE 
    WHEN stock.record_day <= CAST(stock.impossibled_at AS date) THEN NULL
    ELSE stock.impossibled_at END AS sold_date,
  stock.stock_updated_at,
  --stock.impossibled_at AS stock_impossibled_at,
  CASE 
    WHEN stock.record_day <= CAST(stock.impossibled_at AS date) THEN NULL
    ELSE stock.impossibled_at END AS stock_impossibled_at,
  CASE 
    WHEN toB_sold_date <= stock.record_day THEN 'Sold' ELSE NULL END b_status,
  CASE
    WHEN to_b_info.lease_start_at is not null AND to_b_info.lease_start_at <= stock.record_day THEN 1 ELSE 0 END is_lease,
  to_b_info.lease_start_at,
  stock.sample,
  CASE 
    WHEN stock.record_day <= CAST(stock.stock_deleted_at AS date) THEN NULL
    ELSE stock.stock_deleted_at END AS stock_deleted_at
  


FROM
  monthly_latest_stock AS stock
LEFT JOIN
  calendar ON stock.record_day = calendar.record_day
LEFT JOIN
  part_attribute AS part
  ON stock.part_id = part.part_id
LEFT JOIN 
  `lake.supplier` AS supplier
  ON supplier.id = stock.supplier_id
LEFT JOIN 
  lent_info 
  ON lent_info.stock_id = stock.stock_id
LEFT JOIN 
  to_b_info 
  ON to_b_info.stock_id = stock.stock_id
LEFT JOIN 
  to_b_name 
  ON to_b_name.stock_id = stock.stock_id AND to_b_name.start_at = to_b_info.first_shipped_at
LEFT JOIN
  impossibility
  ON impossibility.stock_id = stock.stock_id
)



,calc AS (
SELECT
    base.term,
    base.record_day,
    base.pre_end_term,
    base.beginning_term,
    base.end_term,
    base.stock_id,
    base.part_id,
    base.part_name,
    COALESCE(base.c_category, base.b_category, "") AS category,
    base.depreciation_period,
    CASE
      WHEN sold_date is not null AND sold_date < base.record_day THEN COALESCE(TIMESTAMP_DIFF(sold_date,first_shipped_at, MONTH) + 1, 0)
      ELSE COALESCE(TIMESTAMP_DIFF(cast(base.record_day AS date),first_shipped_at, MONTH) + 1, 0)
    END AS depreciation_months,  -- 1ヶ月未満切り上げ
    CASE
      WHEN sold_date is not null AND sold_date < beginning_term THEN COALESCE(TIMESTAMP_DIFF(sold_date,first_shipped_at, MONTH) + 1, 0)
      ELSE COALESCE(TIMESTAMP_DIFF( beginning_term,first_shipped_at,MONTH) + 1, 0)
    END AS depreciation_beginning_months,  -- 1ヶ月未満切り上げ
    base.supplier_name,
    base.cost,
    base.stock_status,
    base.order_date_to_supplier,
    base.arrival_at,
    base.first_shipped_at,
    base.proposition_name,
    base.impossibility_date,
    base.sold_date,
    base.monthly_depreciation_amount,
    cost - monthly_depreciation_amount * COALESCE(TIMESTAMP_DIFF( beginning_term,first_shipped_at,MONTH) + 1, 0) AS beginning_book_value,
    CASE 
      WHEN beginning_term <= arrival_at AND arrival_at <= end_term THEN cost
      ELSE 0
    END AS increase_value,
    CASE 
      WHEN stock_status = 'Impossibility' AND beginning_term <= stock_impossibled_at AND stock_impossibled_at <= end_term THEN cost
      ELSE 0
    END AS decrease_value,
    CASE
      WHEN sold_date is not null AND sold_date < base.record_day
        THEN monthly_depreciation_amount * COALESCE(TIMESTAMP_DIFF(sold_date,first_shipped_at, MONTH) + 1, 0)
      ELSE monthly_depreciation_amount * COALESCE(TIMESTAMP_DIFF(base.record_day,first_shipped_at, MONTH) + 1, 0)
    END AS accumulated_depreciation_amount,
    cost - monthly_depreciation_amount * COALESCE(TIMESTAMP_DIFF(base.record_day,first_shipped_at, MONTH) + 1, 0) AS month_end_book_value,
    cost - monthly_depreciation_amount * COALESCE(TIMESTAMP_DIFF(end_term,first_shipped_at, MONTH) + 1, 0) AS end_book_value,
    stock_updated_at,
    b_status,
    lease_start_at,
    sample,
    stock_deleted_at,
    is_lease
  FROM 
    base
)

SELECT
  term,--期
  CASE
    WHEN CAST(arrival_at AS date) >= '2018-04-01' AND '2019-04-01' > CAST(arrival_at AS date) THEN 1 
    WHEN CAST(arrival_at AS date) >= '2019-04-01' AND '2020-04-01' > CAST(arrival_at AS date) THEN 2
    WHEN CAST(arrival_at AS date) >= '2020-04-01' AND '2021-03-01' > CAST(arrival_at AS date) THEN 3
    WHEN CAST(arrival_at AS date) >= '2021-03-01' AND '2022-03-01' > CAST(arrival_at AS date) THEN 4
    WHEN CAST(arrival_at AS date) >= '2022-03-01' AND '2023-03-01' > CAST(arrival_at AS date) THEN 5
    WHEN CAST(arrival_at AS date) >= '2023-03-01' AND '2024-03-01' > CAST(arrival_at AS date) THEN 6
    WHEN CAST(arrival_at AS date) >= '2024-03-01' AND '2025-03-01' > CAST(arrival_at AS date) THEN 7
    WHEN CAST(arrival_at AS date) >= '2025-03-01' AND '2026-03-01' > CAST(arrival_at AS date) THEN 8
    WHEN CAST(arrival_at AS date) >= '2026-03-01' AND '2027-03-01' > CAST(arrival_at AS date) THEN 9
    WHEN CAST(arrival_at AS date) >= '2027-03-01' AND '2028-03-01' > CAST(arrival_at AS date) THEN 10
    ELSE NULL 
    END arrival_term,
    
    --入庫した期
  record_day ,--計算基準日（前月末）,
  pre_end_term ,--AS 前期末,
  beginning_term ,--AS 期首,
  end_term ,--AS 今期末,
  CAST(stock_updated_at AS date) AS stock_updated_at,--AS 在庫最終更新日,
  stock_id ,--AS 在庫ID,
  part_id ,--AS パーツID,
  part_name ,--AS パーツ名,
  category ,--AS カテゴリ,
  depreciation_period ,--AS 耐用年数,
  CASE
    WHEN record_day < first_shipped_at THEN 0
    WHEN first_shipped_at = sold_date and sold_date is not null THEN 0
    ELSE depreciation_months
  END AS depreciation_months, --AS 償却済月数
  CASE
    WHEN beginning_term < first_shipped_at THEN 0
    WHEN first_shipped_at = sold_date and sold_date is not null THEN 0
    ELSE depreciation_beginning_months
  END AS depreciation_beginning_months, --（期首時点の）償却済月数,
  supplier_name ,--AS サプライヤー,
  cost ,--AS 仕入金額,
  stock_status,
  CASE stock_status
    WHEN 'Ready' THEN '貸出可能'
    WHEN 'Lending' THEN '貸出中'
    WHEN 'Recovery' THEN 'リカバリー中'
    WHEN 'Impossibility' THEN '破損・紛失'
    WHEN 'LendingToB' THEN 'B向け貸出中'
    ELSE 'その他'
  END AS stock_status_jp,
  order_date_to_supplier ,--AS 発注日,
  arrival_at ,--AS 入荷日,
  first_shipped_at ,--AS 初回出荷日,
  proposition_name, --AS 初回利用の案件名,
  impossibility_date ,--AS 破損紛失処理日,
  sold_date ,--AS 廃棄日, -- 旧）除売却日
  monthly_depreciation_amount ,--AS 月次償却額,
  CASE
    WHEN beginning_book_value < 0 THEN 0
    WHEN sold_date is not null AND sold_date < beginning_term THEN 0
    WHEN beginning_term < arrival_at THEN 0
    WHEN beginning_term < first_shipped_at THEN cost
    ELSE beginning_book_value
  END AS beginning_book_value,--期首帳簿価額,
  increase_value AS increase_value,--AS 期中増加額,
  decrease_value AS decrease_value ,--AS 期中減少額,
  CASE
    WHEN accumulated_depreciation_amount < 0 THEN 0
    WHEN first_shipped_at = sold_date and sold_date is not null THEN 0
    ELSE accumulated_depreciation_amount
  END AS accumulated_depreciation_amount, --償却累計額
  CASE
    WHEN month_end_book_value < 0 THEN 0
    WHEN sold_date is not null AND sold_date < record_day THEN 0
    WHEN record_day < arrival_at THEN 0
    WHEN record_day < first_shipped_at THEN cost
    ELSE month_end_book_value
  END AS month_end_book_value ,--（月末）期末帳簿価額,
  CASE
    WHEN end_book_value < 0 THEN 0
    WHEN sold_date is not null AND sold_date < end_term THEN 0
    WHEN end_term < arrival_at THEN 0
    WHEN end_term < first_shipped_at THEN cost
    ELSE end_book_value
  END AS end_book_value,--期末帳簿価額
    b_status,
    lease_start_at,
    sample,
    stock_deleted_at,
    is_lease
FROM
  calc
WHERE
  term is not null
   --arrival_at >= beginning_term

  );

END;