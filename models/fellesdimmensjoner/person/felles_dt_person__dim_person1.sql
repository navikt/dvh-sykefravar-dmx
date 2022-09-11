WITH source AS (
  SELECT * FROM {{ dbt_test_source('dt_person', 'dim_person1') }}
)

SELECT * FROM source
