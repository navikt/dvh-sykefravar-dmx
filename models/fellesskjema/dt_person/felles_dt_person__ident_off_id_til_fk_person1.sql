WITH source AS (
  SELECT * FROM {{source('dt_person', 'ident_off_id_til_fk_person1')}}
  )

SELECT * FROM source