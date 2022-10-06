WITH hendelser AS (
  SELECT * FROM {{ref("mk_syfo210_hendelser_pvt")}}
)
,dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)
,final AS (
  SELECT
    hendelser.fk_person1
    ,tilfelle_startdato
    ,1 AS dialogmote2_innen_26_uker_flagg
    ,0 AS svar_behov
    ,NULL AS svar_behov_hvem
    ,NULL AS behov_meldt_dato
    ,NULL AS behov_meldt_hvem
    ,dialogmote_tidspunkt1 AS dialogmote2_avholdt_dato
    ,dialogmote_tidspunkt2 AS dialogmote3_avholdt_dato
    ,unntak AS unntak_dato
    ,dim_person1.fk_dim_organisasjon
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1
    AND hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
)
SELECT * FROM final
