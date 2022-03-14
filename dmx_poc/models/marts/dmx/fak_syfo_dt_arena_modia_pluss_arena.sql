

WITH fak_syfo_arena AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),

fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia')}}
),

final AS (
    SELECT  fak_syfo_arena.*,
            fak_syfo_arena_modia.*
from fak_syfo_arena
left join  fak_syfo_arena_modia
on fak_syfo_arena.fk_person1 = fak_syfo_arena.fk_person1
    
)


SELECt final.* FROM final