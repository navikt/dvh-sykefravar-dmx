
with fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia')}}
),

fak_syfo_arena2 AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),
fak_syfo_modia2 AS (
         SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),

final AS (
         SELECT  fak_syfo_arena_modia.*,
                 fak_syfo_arena2.KEY_DMX_ARENA,
                 fak_syfo_modia2.KEY_DMX as key_dmx_modia

         from fak_syfo_arena_modia
         left join fak_syfo_arena2
            on fak_syfo_arena_modia.key_dmx = fak_syfo_arena2.KEY_DMX_ARENA
            -- and  fak_syfo_arena_modia.F_KILDESYSTEM like  'Arena%'
        left join  fak_syfo_modia2
            on  fak_syfo_arena_modia.key_dmx = fak_syfo_modia2.key_dmx
           -- and fak_syfo_arena_modia.f_kildesystem like  'Modia%'
)


SELECt final.* FROM final

