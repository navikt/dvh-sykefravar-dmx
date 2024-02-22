WITH source AS (
  SELECT * FROM {{ source('dvh_sykm', 'fak_sykm_sykefravar_tilfelle') }}
)

SELECT * FROM source
