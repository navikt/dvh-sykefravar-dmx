/********************************************************
View til Team PIA
*********************************************************/
with siste_periode as (
  select max(b.rapport_periode) periode
  from {{ source('dt_kodeverk', 'dim_versjon') }}  b
  join {{ source('dt_kodeverk', 'publiseringstabell') }}  p on
      p.rapport_periode = b.rapport_periode and
      p.tabell_navn = 'IA_PARAM' and
      p.offentlig_dato < sysdate and
      b.tabell_navn = 'FAK_IA_SYKEFRAVAR'
),

sykefravar_statistikk_virksomhet_per_varighet as (
    select
    orgnr,
    arstall,
    kvartal,
    varighet,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(taptedv_gs) taptedv_gs,
    rectype
from {{ source('syfra', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1
where dim.rapport_periode <= (select periode from siste_periode)
and dim.rapport_periode > (select periode - 500 from siste_periode)
group by
    orgnr,
    arstall,
    kvartal,
    varighet,
    rectype
),

sykefravar_statistikk_virksomhet as (
  select
      orgnr,
      arstall,
      kvartal,
      sum(taptedv) taptedv,
      sum(muligedv) muligedv,
      sum(taptedv_gs) taptedv_gs,
      rectype
  from sykefravar_statistikk_virksomhet_per_varighet
  group by
    orgnr,
    arstall,
    kvartal,
    rectype
),

tapte_dagsverk_per_varighet_pivotert as (
  select * from (
    select
      orgnr,
      arstall,
      kvartal,
      varighet,
      taptedv
    from sykefravar_statistikk_virksomhet_per_varighet
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

sykefravar_statistikk_virksomhet_med_varighet as (
  select
    s.orgnr,
    s.arstall,
    s.kvartal,
    round((s.taptedv/NULLIF(s.muligedv, 0)) * 100, 1) as prosent,
    s.taptedv,
    s.muligedv,
    s.taptedv_gs,
    s.rectype as rectype,
    td.varighet_A as varighet_A,
    td.varighet_B as varighet_B,
    td.varighet_C as varighet_C,
    td.varighet_D as varighet_D,
    td.varighet_E as varighet_E,
    td.varighet_F as varighet_F
  from sykefravar_statistikk_virksomhet s
  left join tapte_dagsverk_per_varighet_pivotert td on
    s.orgnr=td.orgnr and s.arstall=td.arstall and s.kvartal=td.kvartal

),

final as (
  select
    orgnr,
    arstall,
    kvartal,
    prosent,
    taptedv,
    muligedv,
    rectype,
    taptedv_gs,
    varighet_A,
    varighet_B,
    varighet_C,
    varighet_D,
    varighet_E,
    varighet_F
  from sykefravar_statistikk_virksomhet_med_varighet
)

select * from final