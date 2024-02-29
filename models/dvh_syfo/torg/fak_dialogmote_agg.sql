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
dim_geografi as (
    SELECT * FROM {{ ref('felles_dt_p__dim_geografi') }}
),
dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
),

dialogmøter_agg AS (
    SELECT
        EXTRACT(YEAR FROM gen_dato.dato) AS aar,
        EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
        TO_CHAR(gen_dato.dato, 'IW') as uke,
        dim_org.nav_enhet_navn,
        dim_org.nav_nivaa3_navn as fylke,
        dim_geografi.fylke_navn,
        dim_geografi.kommune_navn,
        dim_alder.alder as alder,
         CASE
            WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
            WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
            ELSE 'U'
        END as kjonn,
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
    join
        dim_person1  ON fakta_gen.fk_person1 = dim_person1.pk_dim_person and dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
      JOIN
        dim_geografi ON dim_person1.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        and dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')

    GROUP BY
         EXTRACT(YEAR FROM gen_dato.dato),
        EXTRACT(MONTH FROM gen_dato.dato),
        TO_CHAR(gen_dato.dato, 'IW'),
       dim_org.nav_enhet_navn,
       dim_org.nav_nivaa3_navn,
        dim_geografi.fylke_navn,
        dim_geografi.kommune_navn,
       dim_alder.alder,
        CASE
            WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
            WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
            ELSE 'U'
        END
    union
     SELECT
        EXTRACT(YEAR FROM gen_dato.dato) AS aar,
        EXTRACT(MONTH FROM gen_dato.dato) AS maaned,
        TO_CHAR(gen_dato.dato, 'IW') as uke,
        dim_org.nav_enhet_navn,
        dim_org.nav_nivaa3_navn as fylke,
        dim_geografi.fylke_navn,
        dim_geografi.kommune_navn,
        dim_alder.alder as alder,
         CASE
            WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
            WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
            ELSE 'U'
        END as kjonn,
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
    join
        dim_person1  ON fakta_gen.fk_person1 = dim_person1.pk_dim_person
        and  dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
     JOIN
        dim_geografi ON dim_person1.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        and  dim_person1.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
    GROUP BY
         EXTRACT(YEAR FROM gen_dato.dato),
        EXTRACT(MONTH FROM gen_dato.dato),
        TO_CHAR(gen_dato.dato, 'IW'),
       dim_org.nav_enhet_navn,
       dim_org.nav_nivaa3_navn,
      dim_geografi.fylke_navn,
     dim_geografi.kommune_navn,
       dim_alder.alder,
       CASE
            WHEN dim_person1.fk_dim_kjonn = 5002 THEN 'K'
            WHEN dim_person1.fk_dim_kjonn = 5001 THEN 'M'
            ELSE 'U'
        END

)
SELECT * FROM dialogmøter_agg





