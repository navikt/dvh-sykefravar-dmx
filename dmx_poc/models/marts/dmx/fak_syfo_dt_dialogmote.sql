

{{ config(materialized='table') }}



WITH fak_syfo_dialogmote_s AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote')}}
),

dim_varighet AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_varighet')}}
),

final AS (
    SELECT 
           fak_syfo_dialogmote_s.avlyst as avlyst_flagg,
           fak_syfo_dialogmote_s.ferdigstilt as ferdigstilt_flagg,
           fak_syfo_dialogmote_s.innkalt as innkalt_flagg,
           fak_syfo_dialogmote_s.nytt_tid_sted as nytt_tid_sted_flagg,
           fak_syfo_dialogmote_s.ferdigstilt_tidspunkt as avholdt_dialog_tidspunkt,
           fak_syfo_dialogmote_s.avlyst_tidspunkt as avlyst_dialog_tidspunkt,
           fak_syfo_dialogmote_s.innkalt_tidspunkt as innkalt_dialog_tidspunkt,
           fak_syfo_dialogmote_s.arbeidstaker_flagg as arbeidstaker_deltatt_flagg,
           fak_syfo_dialogmote_s.arbeidsgiver_flagg as arbeidsgiver_deltatt_flagg,
           fak_syfo_dialogmote_s.sykmelder_flagg as sykemelder_deltatt_flagg,
           fak_syfo_dialogmote_s.nyeste_tilfelle_startdato,
           fak_syfo_dialogmote_s.nyeste_dialogmote,
           fak_syfo_dialogmote_s.enhet_nr,
           fak_syfo_dialogmote_s.virksomhetsnr,
           fak_syfo_dialogmote_s.dialogmote_uuid,
           fak_syfo_dialogmote_s.fk_person1,
           fak_syfo_dialogmote_s.fk_dim_person,
           fak_syfo_dialogmote_s.fk_dim_tid_tilfelle_startdato,
           dim_varighet.pk_dim_varighet as fk_dim_varighet
           
    FROM fak_syfo_dialogmote_s
    LEFT JOIN dim_varighet
    ON dim_varighet.varighet_dager = trunc(fak_syfo_dialogmote_s.nyeste_dialogmote) - 
    trunc(fak_syfo_dialogmote_s.nyeste_tilfelle_startdato)
)



SELECt final.* FROM final