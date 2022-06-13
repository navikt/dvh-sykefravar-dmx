{{ config(
    tags=["IA_PIA"]
) }}

WITH fak_sykm_sykefravar_tilfelle_dia AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_diagnose')}}
),

dim_tid AS (
    SELECT * FROM {{ref('stg_dim_tid')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_dia.*, dim_tid.uke as lastet_uke ,dim_tid.aar_uke as lastet_aar_uke
    FROM fak_sykm_sykefravar_tilfelle_dia
    LEFT JOIN dim_tid
    ON fak_sykm_sykefravar_tilfelle_dia.lastet_dato=dim_tid.dato

)

SELECt final.* FROM final