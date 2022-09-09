with
tilfeller_fra_kandidat as (
  select
    *
  from  {{ ref('mk_kandidat_join_nav_enhet') }}
)
,
tilfeller_fra_dialogmote as (
  select
    *
  from  {{ ref('fk_modia__dialogmote__dummy__fix202210') }} -- TODO
)
,--ev kan man hente tilfeller fra arena også?
final as (
  select distinct fk_person1, tilfelle_startdato from (
  select
    fk_person1,
    tilfelle_startdato
  from tilfeller_fra_dialogmote
  union
  select
    fk_person1,
    tilfelle_startdato
  from tilfeller_fra_kandidat)
)
select * from final
