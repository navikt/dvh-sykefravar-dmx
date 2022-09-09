with kandidat as (
  select * from {{ ref('mk_kandidat_join_dvh_person_off_id') }}
) ,

dialogmote as (
  select
    fk_person1,
    tilfelle_start_dato,
    dilogmote_tidspunkt,
    enhet_nr
  from {{ ref('fk_modia__dialogmote') }}
  where status_endring_type = 'FERDIGSTILT'
) ,

kandidat_join_dialogmote as (
  select * from
)
