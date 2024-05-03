with uforhet as (
  select * from {{ ref('fk_modia__arbeidsuforhet')}}
),

dialogmoter as (
  select * from  {{ ref('fk_modia__dialogmote') }}
)

/* Joiner uførhet med dialogmoter for å finne nærmeste tilfelle_startdato.
Setter radnumre basert på differansen i tid mellom uførhetsvurdering opprettet og tilfelle startdato.
Nærmeste dato / minst diff får radnummer 1. */
uforhet_tilfelle_startdato as (
  select *
  from (
        select
          uforhet.*,
          dialogmoter.tilfelle_startdato,
          row_number() over(
            partition by uforhet.fk_person1
            order by (uforhet.createdAt) - (dialogmoter.tilfelle_startdato)
          ) rn
        from uforhet
    left join dialogmoter on uforhet.fk_person1 = dialogmoter.fk_person1
    )
  where rn = 1
)

