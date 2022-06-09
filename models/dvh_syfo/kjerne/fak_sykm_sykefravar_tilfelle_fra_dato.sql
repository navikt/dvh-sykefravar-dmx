{{ config(
    tags=["IA_PIA"]
) }}

WITH fak_sykm_sykefravar_tilfelle_lastet_dto AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_lastet_dato')}}
),

dim_tid AS (
    SELECT * FROM {{ref('stg_dim_tid')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_lastet_dto.*, 
    to_number(to_char(fak_sykm_sykefravar_tilfelle_lastet_dto.sykefravar_fra_dato, 'YYYYMMDD')) as fk_dim_tid_tilfelle_startdato
    FROM fak_sykm_sykefravar_tilfelle_lastet_dto

)

SELECt final.* FROM final