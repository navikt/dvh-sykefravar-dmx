{{ config(
    tags=["IA_PIA"]
) }}


WITH fak_sykm_sykefravar_tilfelle_geo AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_geografi')}}
),


dim_arbeidsperiode AS (
    SELECT * FROM {{ref('stg_arbeidsperiode_unique')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_geo.*
    FROM fak_sykm_sykefravar_tilfelle_geo


SELECt final.* FROM final