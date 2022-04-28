WITH godkjente_versjoner AS (
  SELECT pk_dim_versjon FROM {{ source('dt_p', 'dim_versjon') }}
  WHERE
    tabell_navn = 'SSB_SYFRA_TELLER'
    AND status = 'GODKJENT'
),

tellerfil_grunnlag AS (
  SELECT *
  FROM godkjente_versjoner
  LEFT JOIN {{ source('dt_sensitiv', 'ssb_syfra_teller') }} tellerfil_grunnlag
  ON
    tellerfil_grunnlag.fk_dim_versjon = godkjente_versjoner.pk_dim_versjon
),

person_ident AS (
  SELECT *
  FROM {{ source('dt_person', 'dvh_person_ident_off_id') }}
),

final AS (
  SELECT
    tellerfil_grunnlag.pk_ssb_syfra_teller AS fk_ssb_syfra_teller,
    tellerfil_grunnlag.fk_person1,
    person_ident.off_id,
    tellerfil_grunnlag.s_start,
    tellerfil_grunnlag.s_stopp,
    tellerfil_grunnlag.p_start,
    tellerfil_grunnlag.p_slutt,
    tellerfil_grunnlag.gj_uforg,
    tellerfil_grunnlag.kvartal
  FROM tellerfil_grunnlag
  LEFT JOIN person_ident
    ON person_ident.fk_person1 = tellerfil_grunnlag.fk_person1
    AND person_ident.gyldig_fra_dato <= tellerfil_grunnlag.p_start
    AND person_ident.gyldig_til_dato >= tellerfil_grunnlag.p_slutt
)

SELECT * FROM final
