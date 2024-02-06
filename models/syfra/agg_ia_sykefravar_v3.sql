{{ config(
    post_hook= ["grant READ ON {{this}} to DVH_SYFRA_APP"]
)}}


with siste_periode as (
  select max(b.rapport_periode) periode
  from {{ source('dt_kodeverk', 'dim_versjon') }}  b
  join {{ source('dt_kodeverk', 'publiseringstabell') }}  p on
      p.rapport_periode = b.rapport_periode and
      p.tabell_navn = 'IA_PARAM' and
      p.offentlig_dato < sysdate and
      b.tabell_navn = 'FAK_IA_SYKEFRAVAR'
),
agg_ia_sykefravar as (
  select
    arstall,
    kvartal,
    orgnr,
    naring,
    naering_kode_sn2007 naering_kode,
    naring_primar_kode primærnæringskode,
    sektor,
    varighet,
    rectype,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(antall_gs) antall_gs,
    sum(taptedv_gs) taptedv_gs,
    sum(antpers) antpers
from {{ source('dt_p', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1
where dim.rapport_periode <= (select periode from siste_periode)
and dim.rapport_periode > (select periode - 500 from siste_periode)
group by
    arstall,
    kvartal,
    orgnr,
    naring,
    naering_kode_sn2007,
    naring_primar_kode,
    sektor,
    varighet,
    rectype
),
final as(
  select
    arstall,
    kvartal,
    orgnr,
    naring,
    naering_kode,
    primærnæringskode,
    sektor,
    varighet,
    rectype,
    taptedv,
    muligedv,
    antall_gs,
    taptedv_gs,
    antpers
  from agg_ia_sykefravar
  )

select * from final
