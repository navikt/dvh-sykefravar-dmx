{{ config(
    tags="syfra"
) }}

SELECT gj_uforg
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE (LENGTH((gj_uforg) - TRUNC(gj_uforg)) - 1) > 10
