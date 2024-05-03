WITH person_oversikt AS (
  SELECT * FROM  {{ source('modia', 'fk_syfo_person_oversikt_status') }}
  where tildelt_enhet  != 'None'
)
select * from person_oversikt