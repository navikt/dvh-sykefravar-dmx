{{ config(
    materialized='table',
)}}

wITH gen_dato AS (
    SELECT CAST((DATE '2023-01-01' + LEVEL - 1) AS TIMESTAMP) AS dato
    FROM dual
    CONNECT BY LEVEL <= (DATE '2024-12-31' - DATE '2023-01-01' + 1)
),
fakta_gen_org AS (
    SELECT dialogmote2_avholdt_dato,DIALOGMOTE3_AVHOLDT_DATO,unntak_dato,behov_meldt_dato,
    TILFELLE_STARTDATO,fk_dim_organisasjon,fk_dim_alder,fk_person1,fk_dim_naering
    FROM {{ ref('fak_dialogmote') }}
),
fakta_gen AS (
    select fakta_gen_org.*,
    CASE
        WHEN dialogmote2_avholdt_dato IS NULL THEN NULL
        WHEN (trunc(cast(DIALOGMOTE2_AVHOLDT_DATO as date),'d') -
            trunc(cast(TILFELLE_STARTDATO as date),'d')) <= 26*7
        THEN DIALOGMOTE2_AVHOLDT_DATO
        ELSE NULL
    END AS dialogmote2_innen_26_uker_dato,
    CASE
        WHEN dialogmote3_avholdt_dato IS NULL THEN NULL
        WHEN (trunc(cast(DIALOGMOTE3_AVHOLDT_DATO as date),'d') -
            trunc(cast(TILFELLE_STARTDATO as date),'d')) <= 39*7
        THEN DIALOGMOTE3_AVHOLDT_DATO
        ELSE NULL
    END AS dialogmote3_innen_39_uker_dato,
    CASE
        WHEN unntak_dato IS NULL THEN NULL
        WHEN (trunc(cast(unntak_dato as date),'d') -
            trunc(cast(TILFELLE_STARTDATO as date),'d')) <=26*7
        THEN unntak_dato
        ELSE NULL
    END AS unntak_innen_26_uker_dato,
    CASE
        WHEN behov_meldt_dato IS NULL THEN NULL
        WHEN (trunc(cast(behov_meldt_dato as date),'d') -
            trunc(cast(TILFELLE_STARTDATO as date),'d')) < 26*7
        THEN behov_meldt_dato
        ELSE NULL
    END AS behov_meldt_innen_26_uker_dato

    from fakta_gen_org
),
dim_org AS (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_organisasjon') }}
),
dim_geografi as (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_geografi') }}
),
dim_alder AS (
    SELECT *
    FROM {{ ref('felles_dt_kodeverk__dim_alder') }}
),
dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
),
naering as (
    select * from {{ ref('felles_dt_p__dim_naering') }}
),

dialogmøter_agg AS (
SELECT
    EXTRACT(YEAR FROM gen_dato.dato) AS aar,
    EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
    TO_CHAR(gen_dato.dato, 'IW') as uke,
    dim_org.nav_nivaa2_navn as enhet_fylke,
    dim_org.nav_enhet_navn,
    dim_geografi.fylke_navn as bosted_fylke,
    dim_geografi.kommune_navn as bosted_kommune,
    dim_alder.TI_AAR_GRUPPE_BESK as alder_interval,
    naering.gruppe5_besk_lang as naering,
    CASE
        WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
        WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
        ELSE 'U'
    END as kjonn,
    COUNT(CASE WHEN TRUNC(fakta_gen.DIALOGMOTE2_AVHOLDT_DATO) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_dialogmøter2,
    COUNT(CASE WHEN TRUNC(fakta_gen.DIALOGMOTE3_AVHOLDT_DATO) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_dialogmøter3,
    COUNT(CASE WHEN TRUNC(fakta_gen.unntak_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_unntak,
    COUNT(CASE WHEN TRUNC(fakta_gen.dialogmote2_innen_26_uker_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS ant_dialogmote2_innen_26_uker,
    COUNT(CASE WHEN TRUNC(fakta_gen.dialogmote3_innen_39_uker_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS ant_dialogmote3_innen_39_uker,
    COUNT(CASE WHEN TRUNC(fakta_gen.unntak_innen_26_uker_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS ant_unntak_innen_26_uker,
    COUNT(CASE WHEN TRUNC(fakta_gen.behov_meldt_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_behov_meldt,
    COUNT(CASE WHEN TRUNC(fakta_gen.behov_meldt_innen_26_uker_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS ant_behov_meldt_innen_26_uker
FROM
    gen_dato
INNER JOIN
    fakta_gen ON trunc(gen_dato.dato) IN (trunc(fakta_gen.DIALOGMOTE2_AVHOLDT_DATO), trunc(fakta_gen.DIALOGMOTE3_AVHOLDT_DATO),
    trunc(fakta_gen.unntak_dato),trunc(dialogmote2_innen_26_uker_dato), trunc(dialogmote3_innen_39_uker_dato),
    trunc(dialogmote3_innen_39_uker_dato), trunc(unntak_innen_26_uker_dato),trunc(behov_meldt_innen_26_uker_dato), trunc(behov_meldt_dato))
INNER JOIN
    dim_org ON fakta_gen.fk_dim_organisasjon = dim_org.pk_dim_organisasjon
INNER JOIN
    dim_person1  ON fakta_gen.fk_person1 = dim_person1.fk_person1
    and  dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
inner JOIN
        dim_geografi ON dim_person1.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        and  dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
        and  dim_geografi.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
inner join
    dim_alder  ON fakta_gen.fk_dim_alder = dim_alder.pk_dim_alder
inner join
    naering on  fakta_gen.fk_dim_naering = naering.pk_dim_naering
    and naering.gyldig_flagg=1
GROUP BY
    EXTRACT(YEAR FROM gen_dato.dato),
    EXTRACT(MONTH FROM gen_dato.dato),
    TO_CHAR(gen_dato.dato, 'IW'),
    dim_org.nav_nivaa2_navn,
    dim_org.nav_enhet_navn,
    dim_geografi.fylke_navn,
    dim_geografi.kommune_navn,
    dim_alder.ungdomssatsing,
    naering.gruppe5_besk_lang,
    CASE
        WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
        WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
        ELSE 'U'
    END
)
SELECT * FROM dialogmøter_agg




