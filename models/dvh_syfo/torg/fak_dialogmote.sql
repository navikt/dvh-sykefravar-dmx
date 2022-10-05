WITH hendelser AS (
  SELECT
    1234 AS fk_person1
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS tilfelle_startdato
    ,1234 AS nav_enhet
    ,1 AS dialogmote2_innen_26_uker_flagg
    ,0 AS svar_behov
    ,'Behandler/arbeidsgiver/Sykmeldt/tom' AS svar_behov_hvem
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS behov_meldt_dato
    ,'Behandler/arbeidsgiver/Sykmeldt/tom' AS behov_meldt_hvem
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS dialogmote_avholdt_dato
    ,2 AS dialogmote_nr
    ,0 AS unntak
  FROM dual
)
SELECT * FROM hendelser
