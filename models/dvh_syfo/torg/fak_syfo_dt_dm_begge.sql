{{config(
    materialized='table',
    tags="syfo_dialogmote"
)}}

with fak_syfo_arena_modiax AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia')}}
),


final as (
    select fak_syfo_arena_modiax.*,
    '9999' as fk_dim_naering,
    '1' as arbeidstaker_deltatt_flagg
     from  fak_syfo_arena_modiax

 )

SELECt * FROM final

