{{ config(
    tags="syfra"
) }}

SELECT teller.s_start, teller.s_stopp
FROM {{ ref('ssb_syfra_teller_test') }} teller
LEFT JOIN {{ ref('ssb_syfra_teller_test') }} ref ON
  teller.fk_ssb_syfra_teller = ref.fk_ssb_syfra_teller
WHERE teller.fk_ssb_syfra_teller != ref.fk_ssb_syfra_teller
  AND teller.kvartal = {{ var('kvartal', 202101) }}
  AND ref.kvartal = {{ var('kvartal', 202101) }}
  AND teller.fk_person1 = ref.fk_person1
  AND (
    teller.s_start BETWEEN ref.s_start AND ref.s_stopp OR
    teller.s_stopp BETWEEN ref.s_start AND ref.s_stopp OR
    ref.s_start BETWEEN teller.s_start AND teller.s_stopp
  )
