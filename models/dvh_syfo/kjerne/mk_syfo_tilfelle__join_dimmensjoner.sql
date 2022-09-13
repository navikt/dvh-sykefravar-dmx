WITH tilfeller AS (
  SELECT * FROM {{ ref('mk_syfo_tilfelle__join_kandidat_med_dialogmote') }}
)

, dvh_person_ident AS (
  SELECT * FROM {{ ref('felles_dt_person__dvh_person_ident_off_id') }}
)

, dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)

, tilfeller_join_dim_tid AS (
  SELECT
    tilfeller.*
    ,TRUNC(ADD_MONTHS(tilfelle_startdato, + 6), 'MM') AS rapportperiode_start_dato
    ,LAST_DAY(ADD_MONTHS(tilfelle_startdato, + 6)) AS rapportperiode_slutt_dato
    ,TO_NUMBER(CONCAT(TO_CHAR(
      ADD_MONTHS(tilfelle_startdato, + 6), 'YYYYMM'), '003'
    )) AS fk_dim_tid__rapportperiode
    ,TO_NUMBER(TO_CHAR(tilfeller.tilfelle_startdato, 'YYYYMMDD'
    )) AS fk_dim_tid__tilfelle_startdato
  FROM
    tilfeller
)

, tilfeller_join_fk_person1 AS (
  SELECT
    tilfeller_join_dim_tid.*
    ,DECODE(
      dvh_person_ident.fk_person1, NULL, -1, dvh_person_ident.fk_person1
    ) AS fk_person1
  FROM
    tilfeller_join_dim_tid
  LEFT JOIN dvh_person_ident ON
    tilfeller_join_dim_tid.person_ident_number = dvh_person_ident.off_id
  WHERE
    tilfeller_join_dim_tid.rapportperiode_slutt_dato BETWEEN
      dvh_person_ident.gyldig_fra_dato AND dvh_person_ident.gyldig_til_dato OR
    dvh_person_ident.fk_person1 IS NULL
)

, tilfeller_join_dim_organisasjon AS (
  SELECT
    tilfeller_join_fk_person1.*
    ,DECODE(
      dim_person1.fk_dim_organisasjon, NULL, -1, dim_person1.fk_dim_organisasjon
    ) AS fk_dim_organisasjon
  FROM
    tilfeller_join_fk_person1
  LEFT JOIN dim_person1 ON
    tilfeller_join_fk_person1.fk_person1 = dim_person1.fk_person1
  WHERE
    tilfeller_join_fk_person1.rapportperiode_slutt_dato BETWEEN
      dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato OR
    dim_person1.fk_person1 IS NULL
)

, final AS (
  SELECT * FROM tilfeller_join_dim_organisasjon
)

SELECT * FROM final
