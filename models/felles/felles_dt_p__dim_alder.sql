WITH source AS (
  SELECT * FROM {{ source('dt_p', 'dim_alder') }}
)

SELECT * FROM source
