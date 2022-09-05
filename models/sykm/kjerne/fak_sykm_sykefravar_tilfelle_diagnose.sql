WITH fak_sykm_sykefravar_tilfelle_arb AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_arb_sted')}}
),

dim_diagnose AS (
    SELECT * FROM {{ref('stg_dim_diagnose_sykm')}}
),

final AS (
    SELECT fak_sykm_sykefravar_tilfelle_arb.*, dim_diagnose.ICPC_HOVEDGRUPPE_BESK
    FROM fak_sykm_sykefravar_tilfelle_arb
    LEFT JOIN dim_diagnose
    ON fak_sykm_sykefravar_tilfelle_arb.fk_dim_diagnose= dim_diagnose.pk_dim_diagnose

)

SELECt final.* FROM final