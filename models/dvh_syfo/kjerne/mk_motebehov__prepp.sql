WITH motebehov AS (
    SELECT
        motebehov.*,
        case when fk_person1_sm = fk_person1_opprettet_av then 1 else 0 end as behov_sykm,
        case when fk_person1_sm != fk_person1_opprettet_av then 1 else 0 end as behov_arbeidsg
    FROM {{ ref("fk_motebehov")}} motebehov
    where  motebehov.skjematype in  ('MELD_BEHOV', 'SVAR_BEHOV') and --ikke skjematype lik null (der hvor det allerede er kalt inn)
           motebehov.har_motebehov = 1--bare de som har meldt at de har behov
)

,dm AS (
    SELECT * FROM {{ ref("mk_dialogmote__pivotert")}}
)

,motebehov_tilfelle AS (--knytter motebehov til tilfelle, velger først alle tilfeller med startdato foer dato for motebehov, finner maks av disse
    SELECT
        motebehov.*,
        MAX(dm.TILFELLE_STARTDATO) OVER(PARTITION BY dm.fk_person1, motebehov_uuid) AS max_tilfelle
    FROM motebehov
    LEFT JOIN dm ON
        fk_person1 = motebehov.fk_person1_sm AND
        motebehov.opprettet_dato > TILFELLE_STARTDATO
)

,foerste_behov AS (
    SELECT
        MIN(OPPRETTET_DATO) OVER(PARTITION BY fk_person1_sm, max_tilfelle) AS min_opprettet_dato --tidspunkt for det foerste meldte behovet
        ,MAX(behov_sykm) OVER(PARTITION BY fk_person1_sm, max_tilfelle) as behov_sykmeldt
        ,MAX(behov_arbeidsg) OVER(PARTITION BY fk_person1_sm, max_tilfelle) as behov_arbeidsgiver
        ,motebehov_tilfelle.*
    FROM
        motebehov_tilfelle
)
, final as (
SELECT DISTINCT
    fk_person1_sm AS fk_person1
    , max_tilfelle AS tilfelle_startdato
    , behov_sykmeldt
    , behov_arbeidsgiver
    , min_opprettet_dato AS behov_meldt_dato
FROM foerste_behov ORDER BY fk_person1_sm, max_tilfelle )

select * from final
