WITH person_oversikt AS (
  SELECT * FROM  {{ source('dmx_pox_dialogmote', 'fk_syfo_person_oversikt_status') }}
  where tildelt_enhet  != 'None'
)
select * from person_oversikt