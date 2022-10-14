WITH source AS (
  SELECT * FROM {{ source('dt_person', 'dvh_person_ident_off_id') }}
)

SELECT * FROM source
