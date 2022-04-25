WITH godkjente_versjoner AS (
  SELECT pk_dim_versjon FROM {{ source('dt_p', 'dim_versjon') }}
  WHERE
    tabell_navn = 'SSB_SYFRA_TELLER'
    AND status = 'GODKJENT'
),

tellerfil AS (
  SELECT *
  FROM {{ source('dt_sensitiv', 'ssb_syfra_teller') }}
),

final AS (
  SELECT
    tellerfil.fk_person1,
    tellerfil.s_start,
    tellerfil.s_stopp,
    tellerfil.p_start,
    tellerfil.p_slutt,
    tellerfil.gj_uforg,
    tellerfil.kvartal
  FROM godkjente_versjoner
  LEFT JOIN tellerfil ON
    tellerfil.fk_dim_versjon = godkjente_versjoner.pk_dim_versjon
)

SELECT * FROM final
