WITH oversikt_1 AS (
  SELECT * FROM {{ source('modia', 'fk_syfo_person_oversikt_status') }}
)

select * from oversikt_1