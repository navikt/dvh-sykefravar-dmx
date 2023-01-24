WITH motebehov AS (
    SELECT * FROM {{ ref("mk_motebehov__join_fk_person1") }}
)

,dm AS (
    SELECT * FROM {{ ref("mk_dialogmote__pivotert") }}
)

,motebehov_tilfelle AS (
    SELECT
        motebehov.*,
        MAX(dm.TILFELLE_STARTDATO) OVER(PARTITION BY dm.fk_person1, motebehov_uuid) AS max_tilfelle
    FROM motebehov
    LEFT JOIN dm ON
        fk_person1 = motebehov.fk_person1_sm AND
        motebehov.opprettet_dato > TILFELLE_STARTDATO
)

,final AS (
    SELECT
        MAX(HAR_MOTEBEHOV) OVER(PARTITION BY fk_person1_sm, max_tilfelle) AS meld_behov_test
        ,motebehov_tilfelle.*
    FROM
        motebehov_tilfelle
)

,ny_cte AS (
    SELECT
        MIN(behandlet_tidspunkt) OVER(PARTITION BY fk_person1_sm, max_tilfelle) AS min_behandlet_tidspunkt
        ,MIN(OPPRETTET_DATO) OVER(PARTITION BY fk_person1_sm, max_tilfelle) AS min_opprettet_dato
        ,final.*
    FROM
        final
    WHERE har_motebehov = meld_behov_test OR har_motebehov IS NULL AND meld_behov_test IS NULL
)

SELECT DISTINCT
    fk_person1_sm AS fk_person1
    ,max_tilfelle AS tilfelle_startdato
    ,DECODE(min_behandlet_tidspunkt,
        null, null ,1
    ) AS svar_behov
    ,min_behandlet_tidspunkt AS svar_behov_dato
    ,meld_behov_test AS behov_meldt
    ,min_opprettet_dato AS behov_meldt_dato
    ,case when fk_person1_sm = fk_person1_behov then 'Sykmeldte'
     when fk_person1_sm != fk_person1_behov then 'Arbeidsgiver'
     else null
end as behov_meldt_hvem
FROM ny_cte ORDER BY fk_person1_sm, max_tilfelle
