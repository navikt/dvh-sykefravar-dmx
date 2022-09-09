with tilfeller as (
  select * from {{ ref('mk_syfo_tilfeller_drp202210') }}
)

, kandidat as (
  select * from {{ ref('mk_kandidat_join_nav_enhet') }}
)

, tilfelle_m_kandidatdato as (
  select
    tilfeller.fk_person1,
    tilfeller.tilfelle_startdato,
    createdat as kandidatdato,
    kandidat.nav_enhet_kode -- TODO Kanskje endre navn i siste eller første steg?
   from tilfeller
   left join (
      select * from kandidat where kandidat = 1
    ) kandidat
    on (tilfeller.fk_person1 = kandidat.fk_person1 and tilfeller.tilfelle_startdato = kandidat.tilfelle_startdato)
),

final as (
  select
    tilfelle_m_kandidatdato.fk_person1,
    tilfelle_m_kandidatdato.tilfelle_startdato,
    tilfelle_m_kandidatdato.kandidatdato,
    kandidat.createdAt as unntakdato,
    tilfelle_m_kandidatdato.nav_enhet_kode
    from tilfelle_m_kandidatdato left join (select * from kandidat where kandidat = 0 and arsak = 'UNNTAK' ) kandidat
     on (tilfelle_m_kandidatdato.fk_person1 = kandidat.fk_person1 and tilfelle_m_kandidatdato.tilfelle_startdato = kandidat.tilfelle_startdato)
)--ev ta med en select for avholdt dm2 også?
select * from final