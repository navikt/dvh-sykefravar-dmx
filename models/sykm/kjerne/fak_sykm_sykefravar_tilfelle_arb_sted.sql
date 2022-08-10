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
    ON fak_sykm_sykefravar_tilfelle_arbp.KOMMUNE_NUMMER_ARBSTED =
    dim_geografi.kommune_nr and dim_geografi.dim_nivaa= 3 and
    dim_geografi.gyldig_flagg=1 and
    dim_geografi.funk_gyldig_fra_dato <= to_date('2022-01-01', 'YYYY-MM-DD')
  --  and extract(year from dim.geografi.funk_gyldig_fra_dato)=9999
)

SELECt final.* FROM final