WITH source AS (
  -- TODO
  SELECT * FROM {{ dbt_test_source("modia", "dialogmote") }}
)

SELECT * FROM source
