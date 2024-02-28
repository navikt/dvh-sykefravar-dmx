{{ config(
    materialized='view',
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
    SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
),

dim_alder AS (
    SELECT * FROM {{ ref('felles_dt_p__dim_alder') }}
),

dialogmøter_agg AS (
    SELECT
        EXTRACT(YEAR FROM gen_dato.dato) AS aar,
        EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
        TO_CHAR(gen_dato.dato, 'IW') as uke,
        dim_org.nav_enhet_navn,
        dim_alder.alder as alder,
        COUNT(fakta_gen.DIALOGMOTE2_AVHOLDT_DATO) AS antall_dialogmøter2,
        0 as antall_dialogmøter3
    FROM
        gen_dato
    LEFT JOIN
        fakta_gen  ON gen_dato.dato = fakta_gen.DIALOGMOTE2_AVHOLDT_DATO
    JOIN
        dim_org  ON fakta_gen.fk_dim_organisasjon = dim_org.pk_dim_organisasjon
    join
        dim_alder  ON fakta_gen.fk_dim_alder = dim_alder.pk_dim_alder
    GROUP BY
         EXTRACT(YEAR FROM gen_dato.dato),
        EXTRACT(MONTH FROM gen_dato.dato),
        TO_CHAR(gen_dato.dato, 'IW'),
       dim_org.nav_enhet_navn,
       dim_alder.alder
    union all
     SELECT
        EXTRACT(YEAR FROM gen_dato.dato) AS aar,
        EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
        TO_CHAR(gen_dato.dato, 'IW') as uke,
        dim_org.nav_enhet_navn,
        dim_alder.alder as alder,
        0 as antall_dialogmøter2,
        COUNT(fakta_gen.DIALOGMOTE3_AVHOLDT_DATO) AS antall_dialogmøter3
    FROM
        gen_dato
    LEFT JOIN
        fakta_gen  ON gen_dato.dato = fakta_gen.DIALOGMOTE3_AVHOLDT_DATO
    JOIN
        dim_org  ON fakta_gen.fk_dim_organisasjon = dim_org.pk_dim_organisasjon
    join
        dim_alder  ON fakta_gen.fk_dim_alder = dim_alder.pk_dim_alder
    GROUP BY
         EXTRACT(YEAR FROM gen_dato.dato),
        EXTRACT(MONTH FROM gen_dato.dato),
        TO_CHAR(gen_dato.dato, 'IW'),
       dim_org.nav_enhet_navn,
       dim_alder.alder

)
SELECT * FROM dialogmøter_agg





