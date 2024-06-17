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
    sum(taptedv_gs) taptedv_gs
from {{ source('dt_p', 'fak_ia_sykefravar') }} fak
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
    varighet
),

sykefravar_statistikk_virksomhet as (
  select
      orgnr,
      arstall,
      kvartal,
      sum(taptedv) taptedv, -- ROUND her?
      sum(muligedv) muligedv, -- ROUND her?
      sum(taptedv_gs) taptedv_gs
  from sykefravar_statistikk_virksomhet_per_varighet
  group by
    orgnr,
    arstall,
    kvartal
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

mulige_dagsverk_per_varighet_pivotert as (
  select * from (
    select
      orgnr,
      arstall,
      kvartal,
      varighet,
      muligedv
    from sykefravar_statistikk_virksomhet_per_varighet
  )
  PIVOT
  (
    sum(muligedv)
    for varighet in (
     'X' as varighet_X)
  )
),

final as (
  select
    s.orgnr,
    s.arstall,
    s.kvartal,
    round((s.taptedv/s.muligedv) * 100, 1) as prosent,
    s.taptedv,
    s.muligedv,
    s.taptedv_gs,
    round(td.varighet_A, 1) as varighet_A,
    round(td.varighet_B, 1) as varighet_B,
    round(td.varighet_C, 1) as varighet_C,
    round(td.varighet_D, 1) as varighet_D,
    round(td.varighet_E, 1) as varighet_E,
    round(td.varighet_F, 1) as varighet_F,
    round(md.varighet_X, 1) as varighet_X
  from sykefravar_statistikk_virksomhet s
  left join tapte_dagsverk_per_varighet_pivotert td on
    s.orgnr=td.orgnr and s.arstall=td.arstall and s.kvartal=td.kvartal
  left join mulige_dagsverk_per_varighet_pivotert md on
    s.orgnr=md.orgnr and s.arstall=md.arstall and s.kvartal=md.kvartal

)


select * from final
