WITH source AS (
  SELECT * FROM {{ source('dt_hr', 'hr_navkontor_ansatt') }}
)

SELECT * FROM source