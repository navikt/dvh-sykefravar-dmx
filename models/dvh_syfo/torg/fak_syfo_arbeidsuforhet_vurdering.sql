with uforhet as (
  select * from {{ ref('fk_modia__arbeidsuforhet')}}
),

tildelt_enhet as (
  select * from {{ ref('fk_modia__person_oversikt_status_scd') }}
),



joined as (
  select
    uforhet.*,
    tildelt_enhet
  from uforhet
  left join tildelt_enhet on
    uforhet.fk_person1 = tildelt_enhet.fk_person1 and
    uforhet.tilfelle_startdato between tildelt_enhet.gyldig_fra_dato and tildelt_enhet.gyldig_til_dato
)


select * from uforhet