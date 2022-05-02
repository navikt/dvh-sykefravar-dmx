{{ config(
    tags="syfra"
) }}

SELECT
  teller.s_start,
  teller.s_stopp
FROM {{ ref('ssb_syfra_teller_test') }} teller
LEFT JOIN {{ ref('ssb_syfra_teller_test') }} referanse ON
  teller.id = referanse.id
WHERE teller.id != referanse.id
  AND teller.fnr = referanse.fnr
  AND (
    teller.s_start BETWEEN referanse.s_start AND referanse.s_stopp
    OR teller.s_stopp BETWEEN referanse.s_start AND referanse.s_stopp
    OR referanse.s_start BETWEEN teller.s_start AND teller.s_stopp
  )
