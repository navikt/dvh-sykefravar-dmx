{{ config(
    tags="syfra"
) }}

SELECT s_stopp
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE kvartal = {{ var('kvartal', 202101) }}
  AND trunc(s_stopp) > trunc(add_months(lastet_dato, 12))
