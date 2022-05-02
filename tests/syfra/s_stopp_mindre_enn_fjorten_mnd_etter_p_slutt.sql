{{ config(
    tags="syfra"
) }}

SELECT s_stopp
FROM {{ ref('test__ssb_syfra_teller_kv') }}
WHERE
  TRUNC(s_stopp) > TRUNC(ADD_MONTHS(p_slutt, 14))
