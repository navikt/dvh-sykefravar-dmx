WITH source_oppf AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_SYK_LK_SYKMELDER_FNR') }}
)

SELECT * FROM source_oppf
