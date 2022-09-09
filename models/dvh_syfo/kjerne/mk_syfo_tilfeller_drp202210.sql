with
tilfeller_fra_kandidat as (
  select
    fk_person1,
    (createdat - 49) as tilfelle_startdato --må byttes ut
  from  {{ ref('mk_kandidat_join_dvh_person_off_id') }}
)
,
tilfeller_fra_dialogmote as (
  select
    fk_person1,
    tilfelle_startdato
  from  {{ ref('fk_modia__dialogmote') }}
)
,--ev kan man hente tilfeller fra arena også?
final as (
  select distinct fk_person1, tilfelle_startdato from (
  select * from tilfeller_fra_dialogmote
  union
  select * from tilfeller_fra_kandidat)
)
select * from final