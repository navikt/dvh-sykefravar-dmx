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

sykefravar_statistikk_naeringskode_per_varighet as (
  select
    naering_kode_sn2007 as naering_kode,
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
    naering_kode_sn2007,
    arstall,
    kvartal,
    varighet
),

sykefravar_statistikk_naeringskode as (
  select
    naering_kode,
    arstall,
    kvartal,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(taptedv_gs) taptedv_gs,
    sum(antpers) antpers
  from sykefravar_statistikk_naeringskode_per_varighet
  group by
    naering_kode,
    arstall,
    kvartal
),

tapte_dagsverk_per_varighet_pivotert as (
  select * from (
    select
      naering_kode,
      arstall,
      kvartal,
      varighet,
      taptedv
    from sykefravar_statistikk_naeringskode_per_varighet
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

sykefravar_statistikk_naeringskode_med_varighet as (
  select
    s.naering_kode,
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
  from sykefravar_statistikk_naeringskode s
  left join tapte_dagsverk_per_varighet_pivotert td on
    s.naering_kode=td.naering_kode and s.arstall=td.arstall and s.kvartal=td.kvartal

),

final as (
  select
    naering_kode,
    arstall,
    kvartal,
    round(taptedv/muligedv * 100, 1) as prosent,
    taptedv,
    muligedv,
    taptedv_gs,
    varighet_A,
    varighet_B,
    varighet_C,
    varighet_D,
    varighet_E,
    varighet_F,
    antpers
  from sykefravar_statistikk_naeringskode_med_varighet

)

select * from final
