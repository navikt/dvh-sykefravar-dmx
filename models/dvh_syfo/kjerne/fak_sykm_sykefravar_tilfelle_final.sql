{{ config(
    tags=["IA_PIA"],
    materialized='table'
) }}

WITH fak_sykm_sykefravar_tilfelle_til_dto AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_til_dato')}}
),

final AS (

    SELECT * from fak_sykm_sykefravar_tilfelle_til_dto
)

SELECt final.* FROM final