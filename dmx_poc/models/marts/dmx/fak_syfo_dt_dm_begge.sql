{{config(materialized='table')}}


with fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia')}}
),

fak_syfo_arena2 AS (
    SELECT * FROM {{ref('fak_syfo_hendelse_varighet')}}
),
fak_syfo_modia2 AS (
         SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),

final AS (
         SELECT  
                 fak_syfo_arena2.fk_dim_naering,
                 fak_syfo_arena_modia.f_fk_person1,
                 fak_syfo_arena_modia.f_kildesystem,
                 fak_syfo_arena_modia.fk_dim_organisasjon,
                fak_syfo_arena_modia.f_dialog_motedato,
                fak_syfo_arena_modia.fk_dim_varighet,
                fak_syfo_modia2.fk_person1 as fk_person1_m,
                fak_syfo_modia2.arbeidstaker_deltatt_flagg,
                fak_syfo_modia2.fk_dim_varighet as fk_dim_varighet_m
                -- fak_syfo_modia2.KEY_DMX as key_dmx_modia,
                 --fak_syfo_arena_modia.f_fk_person1
         from fak_syfo_arena_modia
         left join fak_syfo_arena2
             on fak_syfo_arena_modia.key_dmx = fak_syfo_arena2.KEY_DMX_ARENA
             and  fak_syfo_arena_modia.F_KILDESYSTEM like  'Arena%'
        left join  fak_syfo_modia2
            on  fak_syfo_arena_modia.key_dmx = fak_syfo_modia2.key_dmx
         and fak_syfo_arena_modia.f_kildesystem like  'Modia%'
)


SELECt final.* FROM final

