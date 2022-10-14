WITH source AS (
  SELECT * FROM {{ source('arena', 'fak_sf_hendelse_dag') }}
)

SELECT * FROM source
