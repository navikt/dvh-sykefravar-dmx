{{ config(
    tags="syfra"
) }}

SELECT teller.fnr
FROM {{ ref('test__ssb_syfra_teller_kv') }} teller
LEFT JOIN
  {{ source('dt_person', 'dvh_person_ident_off_id') }} person_ident ON
    teller.fnr = person_ident.off_id
    AND person_ident.gyldig_fra_dato <= teller.p_slutt + 45
    AND person_ident.gyldig_til_dato >= teller.p_start
WHERE person_ident.skjermet_kode IN (6, 7)
