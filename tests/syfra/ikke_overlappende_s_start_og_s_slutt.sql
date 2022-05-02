{{ config(
    tags="syfra"
) }}

SELECT
  tilfelle.s_start,
  tilfelle.s_stopp
FROM {{ ref('test__ssb_syfra_teller_kv') }} tilfelle
LEFT JOIN {{ ref('test__ssb_syfra_teller_kv') }} ref_tilfelle ON
  tilfelle.id != ref_tilfelle.id
  AND tilfelle.fnr = ref_tilfelle.fnr
WHERE
  tilfelle.s_start BETWEEN ref_tilfelle.s_start AND ref_tilfelle.s_stopp
  OR tilfelle.s_stopp BETWEEN ref_tilfelle.s_start AND ref_tilfelle.s_stopp
