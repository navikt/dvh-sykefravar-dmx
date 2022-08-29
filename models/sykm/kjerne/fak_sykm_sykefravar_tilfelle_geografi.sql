
WITH fak_sykm_sykefravar_tilfelle_org AS (
    SELECT * FROM {{ref('stg_fak_sykm_sykefravar_tilfelle')}}
),

dim_geografi AS (
    SELECT * FROM {{ref('stg_dim_geografi')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_org.*,
    dim_geografi.fylke_navn,
    dim_geografi.kommune_navn
    FROM fak_sykm_sykefravar_tilfelle_org
    LEFT JOIN dim_geografi
    ON fak_sykm_sykefravar_tilfelle_org.fk_dim_geografi = dim_geografi.pk_dim_geografi

)

SELECt final.* FROM final