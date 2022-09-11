WITH source AS (
  SELECT * FROM {{ dbt_test_source("dt_p", "dim_organisasjon") }}
)

SELECT * FROM source
