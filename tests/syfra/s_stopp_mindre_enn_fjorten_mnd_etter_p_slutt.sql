{{ config(
    tags="syfra"
) }}

SELECT s_stopp
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE
  TRUNC(s_stopp) > TRUNC(ADD_MONTHS(p_slutt, 14))
