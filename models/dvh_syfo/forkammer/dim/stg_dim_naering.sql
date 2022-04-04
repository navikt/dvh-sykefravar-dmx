WITH source_dim_naering AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_NAERING') }}
),

final AS (
  SELECT * FROM source_dim_naering
)

SELECT * FROM final
