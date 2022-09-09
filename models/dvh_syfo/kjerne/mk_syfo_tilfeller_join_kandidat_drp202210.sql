with tilfeller as (
  select * from {{ ref('mk_syfo_tilfeller_drp202210') }}
) ,

kandidat as (
  select kandidat.* , (createdat - 49) as tilfelle_startdato from {{ ref('mk_kandidat_join_dvh_person_off_id') }} kandidat

  ),

tilfelle_m_kandidatdato as (
  select
    tilfeller.fk_person1,
    tilfeller.tilfelle_startdato,
    createdat as kandidatdato
   from tilfeller left join (select * from kandidat where kandidat = 1) kandidat
   on (tilfeller.fk_person1 = kandidat.fk_person1 and tilfeller.tilfelle_startdato = kandidat.tilfelle_startdato)
),

final as (
  select
    tilfelle_m_kandidatdato.fk_person1,
    tilfelle_m_kandidatdato.tilfelle_startdato,
    tilfelle_m_kandidatdato.kandidatdato,
    kandidat.createdAt as unntakdato
    from tilfelle_m_kandidatdato left join (select * from kandidat where kandidat = 0 and arsak = 'UNNTAK' ) kandidat
     on (tilfelle_m_kandidatdato.fk_person1 = kandidat.fk_person1 and tilfelle_m_kandidatdato.tilfelle_startdato = kandidat.tilfelle_startdato)
)--ev ta med en select for avholdt dm2 også?
select * from final