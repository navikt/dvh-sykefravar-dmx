{{ config(
    materialized='table',
)}}

wITH gen_dato AS (
    SELECT CAST((DATE '2019-01-01' + LEVEL - 1) AS TIMESTAMP) AS dato
    FROM dual
    CONNECT BY LEVEL <= (DATE '2024-12-31' - DATE '2019-01-01' + 1)
),
fakta_gen_org AS (
    SELECT *
    FROM {{ ref('fak_dialogmote') }}
),
fakta_gen AS (
    select fakta_gen_org.*,
    CASE
        WHEN dialogmote2_avholdt_dato IS NULL THEN 0
        WHEN (trunc(cast(DIALOGMOTE2_AVHOLDT_DATO as date),'d') -
        trunc(cast(TILFELLE_STARTDATO as date),'d')) BETWEEN 0 AND 26*7
        THEN 1
        ELSE 0
    END AS dialogmote2_innen_26_uker_flagg_2
    from fakta_gen_org
),
dim_org AS (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_organisasjon') }}
),

dim_alder AS (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_alder') }}
),
dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
),

dialogmøter_agg AS (
SELECT
    EXTRACT(YEAR FROM gen_dato.dato) AS aar,
    EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
   -- TO_CHAR(gen_dato.dato, 'IW') as uke,
    dim_org.nav_nivaa3_navn as enhet_fylke,
    dim_alder.alder as alder,
    CASE
        WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
        WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
        ELSE 'U'
    END as kjonn,
    COUNT(CASE WHEN TRUNC(fakta_gen.DIALOGMOTE2_AVHOLDT_DATO) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_dialogmøter2,
    COUNT(CASE WHEN TRUNC(fakta_gen.DIALOGMOTE3_AVHOLDT_DATO) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_dialogmøter3,
    COUNT(CASE WHEN TRUNC(fakta_gen.unntak_dato) = TRUNC(gen_dato.dato) THEN 1 END) AS antall_unntak,
    sum(fakta_gen.dialogmote2_innen_26_uker_flagg_2) AS ant_dialogmote2_innen_26_uker
FROM
    gen_dato
INNER JOIN
    fakta_gen ON trunc(gen_dato.dato) IN (trunc(fakta_gen.DIALOGMOTE2_AVHOLDT_DATO), trunc(fakta_gen.DIALOGMOTE3_AVHOLDT_DATO),
    trunc(fakta_gen.unntak_dato))
INNER JOIN
    dim_org ON fakta_gen.fk_dim_organisasjon = dim_org.pk_dim_organisasjon
INNER JOIN
    dim_person1  ON fakta_gen.fk_person1 = dim_person1.fk_person1
     and  dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
inner join
    dim_alder  ON fakta_gen.fk_dim_alder = dim_alder.pk_dim_alder
GROUP BY
    EXTRACT(YEAR FROM gen_dato.dato),
    EXTRACT(MONTH FROM gen_dato.dato),
  --  TO_CHAR(gen_dato.dato, 'IW'),
    dim_org.nav_nivaa3_navn,
    dim_alder.alder,
    CASE
        WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
        WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
        ELSE 'U'
    END
)
SELECT * FROM dialogmøter_agg




