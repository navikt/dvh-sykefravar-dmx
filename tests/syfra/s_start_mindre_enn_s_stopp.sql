{{ config(
    tags="syfra"
) }}


SELECT s_start
FROM {{ ref('test__ssb_syfra_teller_kv') }}
WHERE s_start > s_stopp
