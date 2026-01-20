SELECT
  `在庫ID`,
  `前回保有`,
  `今回保有`,
  `減少額`,
  `除売却理由`
FROM `clas-analytics.sandbox.depreciation_tax_detail_2026_v2`
WHERE `在庫ID` IN (166545, 166546, 158876, 80801, 169856)
ORDER BY `在庫ID`
