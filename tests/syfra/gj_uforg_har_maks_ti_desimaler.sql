SELECT gj_uforg
FROM {{ ref('test__ssb_syfra_teller_kv') }}
WHERE (LENGTH((gj_uforg) - TRUNC(gj_uforg)) - 1) > 10
