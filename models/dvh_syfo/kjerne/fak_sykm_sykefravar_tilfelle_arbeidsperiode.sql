{{ config(
    tags=["IA_PIA"]
) }}


WITH fak_sykm_sykefravar_tilfelle_geo AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_geografi')}}
),


dim_arbeidsperiode AS (
    SELECT * FROM {{ref('stg_arbeidsperiode')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_geo.*, diM_arbeidsperiode.KOMMUNE_NUMMER_ARBSTED
    FROM fak_sykm_sykefravar_tilfelle_geo
    LEFT JOIN dim_arbeidsperiode
    ON fak_sykm_sykefravar_tilfelle_geo.fk_person1= dim_arbeidsperiode.fk_person1

)

SELECt final.* FROM final