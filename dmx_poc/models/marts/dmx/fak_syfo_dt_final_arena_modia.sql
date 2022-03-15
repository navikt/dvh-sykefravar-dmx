
{{config(materialized='table')}}

with fak_syfo_arena_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_arena_modia')}}
),

fak_syfo_modia_org AS (
    SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),


final AS (
    SELECT  
            fak_syfo_arena_modia.*,
            fak_syfo_modia_org.avlyst_flagg,
            fak_syfo_modia_org.ferdigstilt_flagg,
            fak_syfo_modia_org.innkalt_flagg,
            fak_syfo_modia_org.nytt_tid_sted_flagg,
            fak_syfo_modia_org.arbeidstaker_deltatt_flagg,
            fak_syfo_modia_org.arbeidsgiver_deltatt_flagg,
            fak_syfo_modia_org.sykemelder_deltatt_flagg,
            fak_syfo_modia_org.avholdt_dialog_tidspunkt,
            fak_syfo_modia_org.avlyst_dialog_tidspunkt,
            fak_syfo_modia_org.DIALOGMOTE_UUID
from fak_syfo_arena_modia
left join  fak_syfo_modia_org
on fak_syfo_arena_modia.f_fk_person1 = fak_syfo_modia_org.fk_person1 
and fak_syfo_arena_modia.f_kildesystem = 'Modia'
    

)


SELECt final.* FROM final

