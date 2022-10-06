WITH hendelser AS (
  SELECT * FROM {{ref("mk_syfo210_hendelser_pvt")}}
)




,final AS (
  SELECT
    fk_person1
    ,tilfelle_startdato
    ,1234 AS nav_enhet
    ,1 AS dialogmote2_innen_26_uker_flagg
    ,0 AS svar_behov
    ,'Behandler/arbeidsgiver/Sykmeldt/tom' AS svar_behov_hvem
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS behov_meldt_dato
    ,'Behandler/arbeidsgiver/Sykmeldt/tom' AS behov_meldt_hvem
    ,dialogmote_tidspunkt1 AS dialogmote2_avholdt_dato
    ,2 AS dialogmote_nr
    ,0 AS unntak
  FROM hendelser
)
SELECT * FROM final