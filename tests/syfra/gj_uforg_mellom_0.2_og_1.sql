{{ config(
    tags="syfra"
) }}

SELECT gj_uforg
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE gj_uforg IS NULL OR gj_uforg NOT BETWEEN 0.2 AND 1
