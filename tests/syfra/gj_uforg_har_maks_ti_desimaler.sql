{{ config(
    tags="syfra"
) }}

SELECT gj_uforg
FROM {{ ref('ssb_syfra_teller_test') }}
WHERE kvartal = {{ var('kvartal', 202101) }}
  AND (length((gj_uforg) - trunc(gj_uforg)) - 1) > 10
