WITH source AS (
  SELECT * FROM {{ source('dt_person', 'dim_person1') }}
)

SELECT * FROM source
