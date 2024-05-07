with uforhet as (
  select * from {{ ref('mk_arbeidsuforhet__prepp')}}
),

person_oversikt_status as (
  select * from {{ ref('fk_modia__person_oversikt_status_scd') }}
),

sf_oppfolging as (
  select * from {{ ref('felles_dk_p__sf_oppfolging') }}
  where lk_sf_oppfolgingstypekode = 'S'
),

-- dim_sf_hendelsetype må grantes til syfo
sf_hendelse_hendelsetype_39_uker as (
    select * from {{ ref('int__sf_hendelse_hendelsetype_39_uker') }}
),

/* Sykmeldt uten arbeidsgiver-flagg */
sykm_uten_arbgiver_flagg as (
  select *
  from (
        select
          uforhet.*,
          sf_oppfolging.uten_arbgiver_flagg,
          row_number() over(
            partition by uforhet.fk_person1
            order by (uforhet.createdAt) - (sf_oppfolging.endret_kilde_dato)
          ) rn
        from uforhet
    left join sf_oppfolging on uforhet.fk_person1 = sf_oppfolging.fk_person1
      and uforhet.createdAt >= sf_oppfolging.endret_kilde_dato --ok?
    )
  where rn = 1
),

--denne er ikke riktig, får 3 flere rader enn jeg skal ha
sett_39_uker_flagg as (
    select
      uforhet.*,
      case when (sf_hendelse_hendelsetype_39_uker.fk_dim_tid_dato_hendelse > to_number(to_char(tilfelle_startdato, 'YYYYMMDD')))
        then 1 else 0 end as sett_39_uker_flagg
    from uforhet
    left join sf_hendelse_hendelsetype_39_uker on
      uforhet.fk_person1 = sf_hendelse_hendelsetype_39_uker.fk_person1
      and uforhet.tilfelle_startdato between sf_hendelse_hendelsetype_39_uker.gyldig_fra_dato and sf_hendelse_hendelsetype_39_uker.gyldig_til_dato
),

tildelt_enhet_tilfelle_startdato as (
  select
    uforhet.*,
    tildelt_enhet
  from uforhet
  left join person_oversikt_status on
    uforhet.fk_person1 = person_oversikt_status.fk_person1 and
    uforhet.tilfelle_startdato between person_oversikt_status.gyldig_fra_dato and person_oversikt_status.gyldig_til_dato
),

join_alle as (
  select
    a.*,
    b.sett_39_uker_flagg as sett_39_uker_flagg,
    c.tildelt_enhet as tildelt_enhet
  from sykm_uten_arbgiver_flagg a
  left join sett_39_uker_flagg b
    on a.fk_person1 = b.fk_person1 and a.createdAt = b.createdAt and a.vurderingstype = b.vurderingstype
  left join tildelt_enhet_tilfelle_startdato c
    on a.fk_person1 = c.fk_person1 and a.createdAt = c.createdAt and a.vurderingstype = c.vurderingstype
),

final as (
  select
    fk_person1,
    createdAt,
    vurderingstype,
    tildelt_enhet
    tilfelle_startdato,
    sett_39_uker_flagg,
    uten_arbgiver_flagg
  from join_alle
)

select * from final

