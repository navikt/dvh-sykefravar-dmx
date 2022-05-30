{{ config(
    tags=["IA_PIA"]
    mat
) }}

WITH fak_sykm_sykefravar_tilfelle_fra_dto AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_fra_dato')}}
),

dim_tid AS (
    SELECT * FROM {{ref('stg_dim_tid')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_fra_dto.*,
    dim_tid.uke as til_dato_uke ,
    dim_tid.aar_uke as til_dato_aar_uke
    FROM fak_sykm_sykefravar_tilfelle_fra_dto
    LEFT JOIN dim_tid
    ON fak_sykm_sykefravar_tilfelle_fra_dto.sykefravar_til_dato=dim_tid.dato

)

SELECt final.* FROM final