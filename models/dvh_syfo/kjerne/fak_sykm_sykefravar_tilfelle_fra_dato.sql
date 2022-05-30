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
    SELECT fak_sykm_sykefravar_tilfelle_lastet_dto.*, dim_tid.uke as fra_dato_uke ,dim_tid.aar_uke as fra_dato_aar_uke
    FROM fak_sykm_sykefravar_tilfelle_lastet_dto
    LEFT JOIN dim_tid
    ON fak_sykm_sykefravar_tilfelle_lastet_dto.sykefravar_fra_dato=dim_tid.dato

)

SELECt final.* FROM final