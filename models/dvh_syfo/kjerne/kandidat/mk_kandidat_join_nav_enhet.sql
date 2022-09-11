WITH kandidat AS (
  SELECT * FROM {{ ref('mk_kandidat_join_dvh_person_off_id') }}
)

, dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)

, dim_organisasjon AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
)

, kandidat_join_fk_dim_organisasjon AS (
  SELECT
    kandidat.*,
    dim_person1.fk_dim_organisasjon AS fk_dim_organisasjon
  FROM
    kandidat
  LEFT JOIN dim_person1
    ON kandidat.fk_person1 = dim_person1.fk_person1
  WHERE kandidat.createdAt
    BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  OR dim_person1.fk_person1 IS NULL
)

, kandidat_join_nav_enhet AS (
  SELECT
    kandidat_join_fk_dim_organisasjon.*,
    dim_organisasjon.nav_enhet_kode -- TODO: Sjekk om det er riktig kode.
  FROM kandidat_join_fk_dim_organisasjon
  LEFT JOIN dim_organisasjon
    ON kandidat_join_fk_dim_organisasjon.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
)

, final AS (
  SELECT
    uuid,
    fk_person1,
    createdat,
    tilfelle_startdato,
    kandidat,
    arsak,
    nav_enhet_kode
  FROM kandidat_join_nav_enhet
)

SELECT * FROM final
