WITH source_flagg AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_SYK_SMP_FLAGG') }}
)

SELECT * FROM source_flagg
