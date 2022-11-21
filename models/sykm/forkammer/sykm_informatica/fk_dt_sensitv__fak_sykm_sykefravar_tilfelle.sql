WITH source AS (
  SELECT * FROM {{ source('dt_sykm_informatica', 'fak_sykm_sykefravar_tilfelle') }}
)

SELECT * FROM source
