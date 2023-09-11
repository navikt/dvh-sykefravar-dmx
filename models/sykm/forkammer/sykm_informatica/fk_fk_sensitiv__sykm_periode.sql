WITH source AS (
  SELECT * FROM {{ source('dvh_sykm', 'sykm_periode') }}
)

SELECT * FROM source
