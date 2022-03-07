
WITH fak_syfo_dialogmote_s AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote_tid')}}
),

dim_varighet AS (
    SELECT * FROM {{ref('stg_dim_varighet')}}
),

final AS (
    SELECT fak_syfo_dialogmote_s.*,
           dim_varighet.pk_dim_varighet as fk_dim_varighet
    FROM fak_syfo_dialogmote_s
    LEFT JOIN dim_varighet
    ON dim_varighet.varighet_dager = trunc(fak_syfo_dialogmote_s.nyeste_dialogmote) - trunc(fak_syfo_dialogmote_s.nyeste_tilfelle_startdato)
)
/*
legger inn table som target tabell
*/
{{ config(
    materialized="table"
) }}

SELECt final.* FROM final