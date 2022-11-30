WITH source AS (
  SELECT * FROM {{ source('dt_p', 'dim_org') }}
)

SELECT * FROM source
