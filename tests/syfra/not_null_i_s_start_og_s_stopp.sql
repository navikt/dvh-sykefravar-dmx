SELECT *
FROM {{ ref('test__ssb_syfra_teller_kv') }}
WHERE s_stopp is NULL OR s_start is NULL