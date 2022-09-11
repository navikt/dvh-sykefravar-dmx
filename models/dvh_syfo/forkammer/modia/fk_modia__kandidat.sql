WITH source AS (
  -- TODO
  SELECT * FROM {{ dbt_test_source("modia", "kandidat") }}
)

SELECT * FROM source
