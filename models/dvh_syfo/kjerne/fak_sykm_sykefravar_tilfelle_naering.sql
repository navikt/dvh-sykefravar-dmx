{{ config(
    tags=["IA_PIA"]
) }}

WITH fak_sykm_sykefravar_tilfelle_til_dto AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_til_dato')}}
),

dim_naering AS (
    SELECT * FROM {{ref('stg_dim_naering')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_til_dto.*, dim_naering.naering_kode,
    dim_naering.gruppe4_besk_lang as NAERING_BESK_LANG
    FROM fak_sykm_sykefravar_tilfelle_til_dto
    LEFT JOIN dim_naering
     ON fak_sykm_sykefravar_tilfelle_til_dto.fk_dim_naering = dim_naering.pk_dim_naering

)

SELECt final.* FROM final