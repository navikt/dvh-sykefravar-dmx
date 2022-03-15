

WITH fak_syfo_arena AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),

fak_syfo_modia AS (
    SELECT * FROM {{ref('fak_syfo_dt_dialogmote')}}
),

final AS (
    SELECT 
           fak_syfo_arena.fk_person1 as f_fk_person1,
           'Arena' as f_kildesystem,
           fak_syfo_arena.FK_EK_DIM_ORG as f_fk_dim_org,
           fak_syfo_arena.OPPRETTET_DK_SF_HEND_DATO as f_dialog_motedato,
           999999 as fk_dim_varighet
    FROM fak_syfo_arena
    union all 
        select 
          fak_syfo_modia.fk_person1 as f_fk_person1,
          'Modia' as f_kildesystem,
          fak_syfo_modia.EK_ORG_NODE as f_fk_orgnode,
          fak_syfo_modia.ferdigstilt_tidspunkt as f_dialog_motedato,
          fak_syfo_modia.fk_dim_varighet as fk_dim_varighet
    from fak_syfo_modia
    
)


SELECt final.* FROM final