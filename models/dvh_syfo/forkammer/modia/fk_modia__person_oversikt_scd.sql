
WITH person_oversikt_scd AS (
  SELECT * FROM {{ source('modia', 'FK_SYFO_PERSON_OVERSIKT_STATUS__SNAPSHOT_V4') }}
)

select * from person_oversikt_scd

