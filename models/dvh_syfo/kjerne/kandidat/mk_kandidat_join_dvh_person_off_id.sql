WITH kandidat AS (
  SELECT * FROM {{ ref('fk_modia__kandidat') }}
)

, dvh_person_ident_off_id AS (
  SELECT * FROM {{ ref('felles_dt_person__dvh_person_ident_off_id') }}
)

, kandidat_join_fk_person1 AS (
  SELECT
    kandidat.*
    ,DECODE(
      dvh_person_ident_off_id.fk_person1, null, -1,
                                          dvh_person_ident_off_id.fk_person1
    ) AS fk_person1
  FROM kandidat
  LEFT JOIN dvh_person_ident_off_id
  ON kandidat.personIdentNumber = dvh_person_ident_off_id.off_id
  WHERE
    createdAt
      BETWEEN dvh_person_ident_off_id.gyldig_fra_dato
      AND dvh_person_ident_off_id.gyldig_til_dato
  OR
    dvh_person_ident_off_id.fk_person1 IS NULL
)

, final AS (
  SELECT
    uuid,
    fk_person1,
    createdAt,
    tilfelle_startdato,
    kandidat,
    arsak
  FROM kandidat_join_fk_person1
)

SELECT * FROM final
