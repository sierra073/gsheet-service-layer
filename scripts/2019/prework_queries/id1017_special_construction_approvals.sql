with special as (
  SELECT
    distinct funding_year,
    funding_status,
    frn,
    original_requested_amount,
    committed_amount

  FROM
    dm.special_construction_services_received

  WHERE
    funding_status in ('Denied','Funded')

  ORDER BY frn asc
)

SELECT
  funding_year,
  SUM(CASE
      WHEN funding_status = 'Denied' THEN original_requested_amount
    ELSE 0 END) AS requested_denied,
  SUM(CASE
        WHEN funding_status = 'Funded' THEN original_requested_amount
      ELSE 0 END) AS requested_funded,
  SUM(CASE
          WHEN funding_status = 'Denied' THEN 1
        ELSE 0 END) AS frns_denied,
  SUM(CASE
    WHEN funding_status = 'Funded' THEN 1
  ELSE 0 END) AS frns_funded

FROM
  special

GROUP BY
  funding_year

ORDER BY
  funding_year asc
