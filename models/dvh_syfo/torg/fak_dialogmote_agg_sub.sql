{{ config(
    materialized='table',
)}}

wITH gen_dato AS (
    SELECT CAST((DATE '2019-01-01' + LEVEL - 1) AS TIMESTAMP) AS dato
    FROM dual
    CONNECT BY LEVEL <= (DATE '2024-12-31' - DATE '2019-01-01' + 1)
),
fakta_gen AS (
    SELECT * FROM {{ ref('fak_dialogmote') }}
),
dim_org AS (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_organisasjon') }}
),

dim_alder AS (
    SELECT * FROM {{ ref('felles_dt_kodeverk__dim_alder') }}
),

dialogmøter_agg AS (
SELECT
    EXTRACT(YEAR FROM gen_dato.dato) AS aar,
    EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
    TO_CHAR(gen_dato.dato, 'IW') as uke,
    dim_org.nav_enhet_navn,
    dim_alder.alder as alder,
    COUNT(CASE WHEN fakta_gen.DIALOGMOTE2_AVHOLDT_DATO = gen_dato.dato THEN 1 END) AS antall_dialogmøter2,
    COUNT(CASE WHEN fakta_gen.DIALOGMOTE3_AVHOLDT_DATO = gen_dato.dato THEN 1 END) AS antall_dialogmøter3
FROM
    gen_dato
INNER JOIN
    fakta_gen ON gen_dato.dato IN (fakta_gen.DIALOGMOTE2_AVHOLDT_DATO, fakta_gen.DIALOGMOTE3_AVHOLDT_DATO)
INNER JOIN
    dim_org ON fakta_gen.fk_dim_organisasjon = dim_org.pk_dim_organisasjon
INNER JOIN
    dim_alder ON fakta_gen.fk_dim_alder = dim_alder.pk_dim_alder
GROUP BY
    EXTRACT(YEAR FROM gen_dato.dato),
     EXTRACT(MONTH FROM gen_dato.dato),
     TO_CHAR(gen_dato.dato, 'IW') ,
    dim_org.nav_enhet_navn,
    dim_alder.alder
)
SELECT * FROM dialogmøter_agg




