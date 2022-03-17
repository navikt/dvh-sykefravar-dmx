



WITH fak_syfo_hendelse_dag AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),

dim_varighet AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_varighet')}}
),

final AS (
    SELECT 
           fak_syfo_hendelse_dag.*,
           dim_varighet.pk_dim_varighet as fk_dim_varighet
    
    FROM fak_syfo_hendelse_dag
    LEFT JOIN dim_varighet
    ON dim_varighet.varighet_dager = 
    trunc(fak_syfo_hendelse_dag.fK_DIM_TID_DATO_HENDELSE - fak_syfo_hendelse_dag.FK_DIM_TID_IDENT_DATO) 
)

SELECt final.* FROM final
