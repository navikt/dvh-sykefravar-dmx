{{ config(
    tags="syfra"
) }}


SELECT s_start
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE s_start > s_stopp
