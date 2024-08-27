/********************************************************
View til Team PIA
*********************************************************/
{{ config(
    post_hook= ["grant READ ON {{this}} to DVH_SYK_DBT"]
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

sykefravar_statistikk_land as (
  select
    'NO' as land,
    arstall,
    kvartal,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(antpers) antpers
from {{ source('syfra', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1
where dim.rapport_periode <= (select periode from siste_periode)
and dim.rapport_periode > (select periode - 500 from siste_periode)
-- Filtrerer for kun rectype 2: VIRKSOMHET (B-nummer), tilsvarende offisiell sykefraværsstatistikk
and fak.rectype = 2
group by
    arstall,
    kvartal
),

final as (
  select
    cast(land as varchar2(100)) as land,
    cast(arstall as number) as arstall,
    cast(kvartal as number) as kvartal,
    cast(round(taptedv/muligedv * 100, 1) as number) as prosent,
    cast(taptedv as number) as taptedv,
    cast(muligedv as number) as muligedv,
    cast(antpers as number) as antpers
  from sykefravar_statistikk_land
)

select
  land,
  arstall,
  kvartal,
  prosent,
  taptedv,
  muligedv,
  antpers
from final