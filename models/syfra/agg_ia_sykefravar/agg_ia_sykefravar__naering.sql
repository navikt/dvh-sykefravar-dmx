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

sykefravar_statistikk_naering_per_varighet as (
  select
    naring,
    arstall,
    kvartal,
    varighet,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(taptedv_gs) taptedv_gs,
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
    naring,
    arstall,
    kvartal,
    varighet
),

sykefravar_statistikk_naering as (
  select
    naring,
    arstall,
    kvartal,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(taptedv_gs) taptedv_gs,
    sum(antpers) antpers
  from sykefravar_statistikk_naering_per_varighet
  group by
    naring,
    arstall,
    kvartal
),

tapte_dagsverk_per_varighet_pivotert as (
  select * from (
    select
      naring,
      arstall,
      kvartal,
      varighet,
      taptedv
    from sykefravar_statistikk_naering_per_varighet
  )
  PIVOT
  (
    sum(taptedv)
    for varighet in (
      'A' as varighet_A,
      'B' as varighet_B,
      'C' as varighet_C,
      'D' as varighet_D,
      'E' as varighet_E,
      'F' as varighet_F)
  )

),

sykefravar_statistikk_naering_med_varighet as (
  select
    s.naring,
    s.arstall,
    s.kvartal,
    round((s.taptedv/NULLIF(s.muligedv, 0)) * 100, 1) as prosent,
    s.taptedv,
    s.muligedv,
    s.taptedv_gs,
    s.antpers as antpers,
    td.varighet_A as varighet_A,
    td.varighet_B as varighet_B,
    td.varighet_C as varighet_C,
    td.varighet_D as varighet_D,
    td.varighet_E as varighet_E,
    td.varighet_F as varighet_F
  from sykefravar_statistikk_naering s
  left join tapte_dagsverk_per_varighet_pivotert td on
    s.naring=td.naring and s.arstall=td.arstall and s.kvartal=td.kvartal

),

final as (
  select
    cast(naring as varchar2(100)) as naring,
    cast(arstall as number) as arstall,
    cast(kvartal as number) as kvartal,
    cast(round(taptedv/muligedv * 100, 1) as number)  as prosent,
    cast(taptedv as number) as taptedv,
    cast(muligedv as number) as muligedv,
    cast(taptedv_gs as number) as taptedv_gs,
    cast(varighet_A as number) as varighet_A,
    cast(varighet_B as number) as varighet_B,
    cast(varighet_C as number) as varighet_C,
    cast(varighet_D as number) as varighet_D,
    cast(varighet_E as number) as varighet_E,
    cast(varighet_F as number) as varighet_F,
    cast(antpers as number) as antpers
  from sykefravar_statistikk_naering_med_varighet
)

select
  naring,
  arstall,
  kvartal,
  prosent,
  taptedv,
  muligedv,
  varighet_A,
  varighet_B,
  varighet_C,
  varighet_D,
  varighet_E,
  varighet_F,
  antpers
from final