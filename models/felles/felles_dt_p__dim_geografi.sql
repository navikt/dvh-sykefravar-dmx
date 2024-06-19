WITH source AS (
  SELECT * FROM {{ source('dt_kodeverk', 'dim_geografi') }}
)

SELECT * FROM source
