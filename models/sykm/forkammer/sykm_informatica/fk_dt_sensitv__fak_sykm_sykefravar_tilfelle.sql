WITH source AS (
  SELECT * FROM {{ source('dt_sensitiv', 'fak_sykm_sykefravar_tilfelle') }}
)

SELECT * FROM source
