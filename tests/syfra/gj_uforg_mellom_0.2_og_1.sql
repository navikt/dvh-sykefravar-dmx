{{ config(
    tags="syfra"
) }}

SELECT gj_uforg
FROM {{ ref('test__ssb_syfra_teller_kv') }}
WHERE gj_uforg NOT BETWEEN 0.2 AND 1
