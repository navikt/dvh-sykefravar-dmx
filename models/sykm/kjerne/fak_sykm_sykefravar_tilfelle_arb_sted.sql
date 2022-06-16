{{ config(
    tags=["IA_PIA"]
) }}

WITH fak_sykm_sykefravar_tilfelle_arbp AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_arbeidsperiode')}}
),

dim_geografi AS (
    SELECT * FROM {{ref('stg_dim_geografi')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_arbp.*,
    dim_geografi.fylke_navn as fylke_arbsted,
    dim_geografi.kommune_navn as kommune_arbsted
    FROM fak_sykm_sykefravar_tilfelle_arbp
    LEFT JOIN dim_geografi
    ON fak_sykm_sykefravar_tilfelle_arbp.fk_dim_geografi =
    dim_geografi.FK_KOMMUNE_ARBSTED

)

SELECt final.* FROM final