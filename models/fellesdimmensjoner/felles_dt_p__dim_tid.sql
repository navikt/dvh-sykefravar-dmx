WITH source AS (
  SELECT * FROM {{ dbt_test_source('dt_p', 'dim_tid') }}
)

SELECT * FROM source
