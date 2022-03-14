
{{config(materialized='table')}}

WITH fak_syfo_modia_org AS (
    SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),

fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia_pluss_arena')}}
),

final AS (
    SELECT  fak_syfo_modia_org.avlyst_flagg,
            fak_syfo_modia_org.ferdigstilt_flagg,
            fak_syfo_modia_org.innkalt_flagg,
            fak_syfo_modia_org.nytt_tid_sted_flagg,
            fak_syfo_modia_org.avholdt_dialog_tidspunkt,
            fak_syfo_modia_org.avlyst_dialog_tidspunkt,
            fak_syfo_modia_org.DIALOGMOTE_UUID,	
            fak_syfo_arena_modia.*
from fak_syfo_modia_org
left join  fak_syfo_arena_modia
on fak_syfo_modia_org.fk_person1 = fak_syfo_arena_modia.f_fk_person1
    
)


SELECt final.* FROM final

