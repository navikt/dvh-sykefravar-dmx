WITH source AS (
  SELECT * FROM {{ source('arena', 'dim_sf_aarsak') }}
)

SELECT * FROM source
