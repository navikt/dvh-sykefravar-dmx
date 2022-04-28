{{ config(
    tags="syfra"
) }}

SELECT test_table.fk_person1
FROM {{ ref('ssb_syfra_teller_test') }} test_table
LEFT JOIN
  {{ source('dt_person', 'dvh_person_ident_off_id') }} person_ident ON
    test_table.fk_person1 = person_ident.fk_person1
    AND person_ident.gyldig_fra_dato <= test_table.p_start
    AND person_ident.gyldig_til_dato >= test_table.p_slutt
WHERE test_table.kvartal = {{ var('kvartal', 202101) }}
  AND person_ident.skjermet_kode IN (6, 7)
