WITH hendelser AS (
  SELECT * FROM {{ref("mk_syfo210_hendelser_pvt")}}
)
,dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)
,flag_innen_26Uker as (
  select fk_person1, tilfelle_startdato,
  CASE
      WHEN dialogmote_tidspunkt1 IS NULL THEN NULL
      WHEN dialogmote_tidspunkt1 < (tilfelle_startdato + 26*7) THEN 1
      ELSE 0
    END AS dialogmote2_innen_26_uker_flagg
   from hendelser
)

,final AS (
  SELECT
    hendelser.fk_person1
    ,hendelser.tilfelle_startdato
    ,dialogmote2_innen_26_uker_flagg
    ,0 AS svar_behov
    ,NULL AS svar_behov_hvem
    ,NULL AS behov_meldt_dato
    ,NULL AS behov_meldt_hvem
    ,dialogmote_tidspunkt1 AS dialogmote2_avholdt_dato
    ,dialogmote_tidspunkt2 AS dialogmote3_avholdt_dato
    ,unntak AS unntak_dato
    ,dim_person1.fk_dim_organisasjon
    ,TO_NUMBER(
      TO_CHAR(hendelser.tilfelle_startdato, 'YYYYMMDD')
    ) AS fk_dim_tid__tilfelle_startdato
    ,TO_NUMBER(
      TO_CHAR(dialogmote_tidspunkt1, 'YYYYMMDD')
    ) AS fk_dim_tid__dialogmote2_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote_tidspunkt2, 'YYYYMMDD')
    ) AS fk_dim_tid__dialogmote3_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(unntak, 'YYYYMMDD')
    ) AS fk_dim_tid__unntak_dato

  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1
    AND hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  left join flag_innen_26Uker on
    hendelser.fk_person1 = flag_innen_26Uker.fk_person1
    and hendelser.tilfelle_startdato = flag_innen_26Uker.tilfelle_startdato
)

SELECT * FROM final
