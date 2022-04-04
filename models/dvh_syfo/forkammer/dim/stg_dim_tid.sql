WITH source_dim_tid AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_TID') }}
),

final AS (
  SELECT * FROM source_dim_tid
)

SELECT * FROM final
