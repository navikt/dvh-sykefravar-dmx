WITH oversikt_raw AS (
  SELECT * FROM {{ source('modia', 'raw_syfo_person_oversikt_status') }}
)

select * from oversikt_raw