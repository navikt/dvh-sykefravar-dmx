{{ config(
    tags="syfra"
) }}

SELECT off_id
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE kvartal = {{ var('kvartal', 202101) }}
  AND off_id IS NULL
