{{ config(
    materialized='view',
)}}

wITH generell_dato AS (
    SELECT generate_series(
        DATE '2022-01-01',  -- Startdato
        DATE '2022-12-31',  -- Sluttdato
        INTERVAL '1 day'
    )::DATE AS dato
),
fakta_gen AS (
    SELECT * FROM {{ ref('fak_dialogmote') }}
),
dim_org AS (
    SELECT * FROM {{ ref('dim_org') }}
),

dialogmøter_agg AS (
    SELECT
        EXTRACT(YEAR FROM g.dato) AS år,
        EXTRACT(MONTH FROM g.dato) AS måned,
        EXTRACT(WEEK FROM g.dato) AS uke,
        d.enhetsnavn,
        a.alder,
        COUNT(*) AS antall_dialogmøter,
        COUNT(f.date_dialogmote2) AS antall_date_dialogmote2
    FROM
        generell_dato g
    LEFT JOIN
        fakta_gen f ON g.dato = f.dato_dm2 OR g.dato = f.date_dialogmote2
    JOIN
        dim_org d ON f.fk_dim_organisasjon = d.org_id
    GROUP BY
        EXTRACT(YEAR FROM g.dato),
        EXTRACT(MONTH FROM g.dato),
        EXTRACT(WEEK FROM g.dato),
        d.enhetsnavn

)
SELECT * FROM dialogmøter_agg



