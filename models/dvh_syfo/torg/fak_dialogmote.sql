{{ config(
    materialized='table'
)}}


WITH hendelser AS (
  SELECT * FROM {{ref("mk_dialogmote__pivotert")}}
)

,dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)

,dim_organisasjon AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
)

,flag_innen_26Uker AS (
  SELECT fk_person1, tilfelle_startdato,
    CASE
      WHEN dialogmote_tidspunkt1 IS NULL THEN NULL
      WHEN dialogmote_tidspunkt1 < (tilfelle_startdato + 26*7) THEN 1
      ELSE 0
    END AS dialogmote2_innen_26_uker_flagg
  FROM hendelser
)

,final AS (
  SELECT
    hendelser.fk_person1
    ,hendelser.tilfelle_startdato
    ,dialogmote2_innen_26_uker_flagg AS dm2_innen_26_uker_flagg
    ,' ' AS svar_behov
    ,' ' AS svar_behov_hvem
    ,' ' AS behov_meldt_dato
    ,' ' AS behov_meldt_hvem
    ,dialogmote_tidspunkt1 AS dialogmote2_avholdt_dato
    ,dialogmote_tidspunkt2 AS dialogmote3_avholdt_dato
    ,unntak AS unntak_dato
    ,TRUNC(hendelser.tilfelle_startdato + 26*7, 'MM') AS tilfelle_26uker_mnd_startdato
    ,dim_organisasjon.nav_enhet_kode_navn
    ,dim_person1.fk_dim_organisasjon
    ,TO_NUMBER(
      TO_CHAR(hendelser.tilfelle_startdato, 'YYYYMMDD')
    ) AS fk_dim_tid__tilfelle_startdato
    ,TO_NUMBER(
      TO_CHAR(dialogmote_tidspunkt1, 'YYYYMMDD')
    ) AS fk_dim_tid__dm2_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote_tidspunkt2, 'YYYYMMDD')
    ) AS fk_dim_tid__dm3_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(unntak, 'YYYYMMDD')
    ) AS fk_dim_tid__unntak_dato
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1 AND
    hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  LEFT JOIN flag_innen_26Uker ON
    hendelser.fk_person1 = flag_innen_26Uker.fk_person1 AND
    hendelser.tilfelle_startdato = flag_innen_26Uker.tilfelle_startdato
  LEFT JOIN dim_organisasjon ON
    dim_person1.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
)

SELECT * FROM final
