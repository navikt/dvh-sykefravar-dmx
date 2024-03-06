WITH source AS (
  SELECT * FROM {{source('dt_person', 'IDENT_OFF_ID_TIL_FK_PERSON1')}}
  )

SELECT * FROM source