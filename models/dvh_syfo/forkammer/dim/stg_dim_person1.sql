WITH source_dim_person1 AS (
  SELECT * FROM {{ source('dmx_poc_person', 'DIM_PERSON1') }}
),

final AS (
  SELECT * FROM source_dim_person1
)

SELECT * FROM final
