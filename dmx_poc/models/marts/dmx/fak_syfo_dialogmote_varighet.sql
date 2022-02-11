WITH fak_syfo_dialogmote_varighet AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote_tid')}}
),

dim_varighet AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_varighet')}}
),

final AS (
    SELECT 
    varighet.*, 
    dim_varighet.pk_dim_varighet as fk_dim_varighet 
    FROM fak_syfo_dialogmote_varighet varighet
    LEFT JOIN dim_varighet on dim_varighet.varighet_dager = trunc(varighet.dialogmote_tidspunkt) - trunc(varighet.tilfelle_startdato)
)

SELECt final.* FROM final