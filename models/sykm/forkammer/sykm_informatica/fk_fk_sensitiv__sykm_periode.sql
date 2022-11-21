WITH source AS (
  SELECT * FROM {{ source('fk_sensitiv', 'sykm_periode') }}
)

SELECT * FROM source
