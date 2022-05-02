{{ config(
    tags="syfra"
) }}

SELECT fnr
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE fnr IS NULL
