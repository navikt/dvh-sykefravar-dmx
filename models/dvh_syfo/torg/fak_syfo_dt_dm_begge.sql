{{config(
    materialized='table',
    tags="syfo_dialogmote"
)}}

with fak_syfo_arena_modiax AS (
    SELECT * FROM {{ref('mk_syfo_arena_union_modia')}}
),
/*
with fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('mk_syfo_arena_union_modia')}}
),

fak_syfo_arena2 AS (
    SELECT * FROM {{ref('mk_sf_hendelse_varighet')}}
),
fak_syfo_modia2 AS (
         SELECT * FROM {{ref('mk_syfo_dialogmote_join_dim_varighet')}}
),

final AS (
         SELECT

                 fak_syfo_arena_modia.f_fk_person1,
                 fak_syfo_arena_modia.f_kildesystem,
                 fak_syfo_arena_modia.fk_dim_organisasjon,
                fak_syfo_arena_modia.f_dialog_motedato,
                fak_syfo_arena_modia.fk_dim_varighet,
                 fak_syfo_arena2.fk_dim_naering,
               -- fak_syfo_modia2.fk_person1 as fk_person1_m,
                fak_syfo_modia2.arbeidstaker_deltatt_flagg
               -- fak_syfo_modia2.fk_dim_varighet as fk_dim_varighet_m
         from fak_syfo_arena_modia
         left join fak_syfo_arena2
             on fak_syfo_arena_modia.key_dmx = fak_syfo_arena2.KEY_DMX_ARENA
             and  fak_syfo_arena_modia.F_KILDESYSTEM like  'Arena%'
        left join  fak_syfo_modia2
            on  fak_syfo_arena_modia.key_dmx = fak_syfo_modia2.key_dmx
         and fak_syfo_arena_modia.f_kildesystem like  'Modia%'
)

*/

final as (
    select fak_syfo_arena_modiax.*,
    '9999' as fk_dim_naering,
    '1' as arbeidstaker_deltatt_flagg
     from  fak_syfo_arena_modiax

 )

SELECt * FROM final

