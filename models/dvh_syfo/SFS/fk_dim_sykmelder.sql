WITH source AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_SYKMELDER') }}
)

SELECT * FROM source
