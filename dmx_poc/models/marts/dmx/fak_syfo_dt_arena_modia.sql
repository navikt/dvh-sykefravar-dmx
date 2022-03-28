

WITH fak_syfo_arena AS (
    SELECT * FROM {{ref('fak_syfo_hendelse_varighet')}}
),

fak_syfo_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),

final AS (
    SELECT 
           fak_syfo_arena.fk_person1 as f_fk_person1,
           'Arena' as f_kildesystem,
          -- fak_syfo_arena.FK_EK_DIM_ORG as f_fk_orgnode,
           fak_syfo_arena.lk_sf_oppfolging_id,
           fak_syfo_arena.fk_dim_organisasjon,
           fak_syfo_arena.key_dmx_arena as key_dmx,
         --  fak_syfo_arena.OPPRETTET_DK_SF_HEND_DATO as f_dialog_motedato,
           to_date(to_char(fk_dim_tid_dato_hendelse), 'YYYY-MM-DD HH24:MI:SS')  as f_dialog_motedato,
           fak_syfo_arena.fk_dim_varighet as fk_dim_varighet
    FROM fak_syfo_arena
    union all
        select 
          fak_syfo_modia.fk_person1 as f_fk_person1,
          'Modia' as f_kildesystem,
          9999999 as id_modia,
         -- fak_syfo_modia.EK_ORG_NODE as f_fk_orgnode,
          fak_syfo_modia.fk_dim_organisasjon,
          fak_syfo_modia.key_dmx,
         fak_syfo_modia.avholdt_dialog_tidspunkt as f_dialog_motedato,
         -- fak_syfo_modia.nyeste_dialogmote as f_dialog_motedato,
          --  fak_syfo_modia.nyeste_TILFELLE_STARTDATO as f_dialog_motedato, 
          fak_syfo_modia.fk_dim_varighet as fk_dim_varighet
    from fak_syfo_modia
    
)


SELECt final.* FROM final