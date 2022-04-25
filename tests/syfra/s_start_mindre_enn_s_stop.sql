{{ config(
    tags="syfra"
) }}


SELECT s_start
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE kvartal = {{ var('kvartal', 202101) }}
  AND s_start > s_stopp
