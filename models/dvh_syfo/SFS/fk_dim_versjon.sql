WITH source AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_VERSJON') }}
)

SELECT * FROM source
