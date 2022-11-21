WITH source AS (
  SELECT * FROM {{ source('fk_sykm_informatica', 'sykm_periode') }}
)

SELECT * FROM source
