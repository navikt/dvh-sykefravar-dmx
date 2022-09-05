WITH kandidat AS (
  SELECT * FROM {{ ref('mk_kandidat_join_dvh_person_off_id') }}
)

, dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)

, kandidat_join_fk_dim_organisasjon AS (
  SELECT
    kandidat.*,
    dim_person1.fk_dim_organisasjon
  FROM
    kandidat
  LEFT JOIN dim_person1
  ON kandidat.fk_person1 = dim_person1.fk_person1
  WHERE kandidat.createdAt
  BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  OR dim_person1.fk_person1 IS NULL
)

, final AS (
  SELECT
    uuid,
    fk_person1,
    createdat,
    kandidat,
    arsak,
    38236 AS fk_dim_organisasjon -- TODO
  FROM kandidat_join_fk_dim_organisasjon
)

SELECT * FROM final