WITH source AS (
  SELECT * FROM {{ source('dt_p', 'dim_organisasjon') }}
)

SELECT * FROM source
