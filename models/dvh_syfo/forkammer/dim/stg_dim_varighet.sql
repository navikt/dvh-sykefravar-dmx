WITH source_dim_varighet AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_VARIGHET') }}
),

final AS (
  SELECT * FROM source_dim_varighet
)

SELECT * FROM final
