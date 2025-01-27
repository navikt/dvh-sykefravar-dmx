/********************************************************
View til Team PIA
Post hook for at viewene kan genereres fra Airflow
*********************************************************/
{{ config(
    post_hook= ["grant READ ON {{this}} to DVH_SYK_DBT"]
)}}

/* Henter siste offentliggjorte periode som har tilgjengeliggjorte data.
Dersom data fra perioden ikke er offentlig enda, hentes ikke disse dataene.
Publiseringsdatoer ligger i publiseringstabellen, og det gjøres en sjekk på om
denne datoene har blitt passert ved generering av viewet.
Egentlig tilstrekkelig å se på dim_versjon siden en Informatica-jobb sjekker mot
publiseringstabellen for å sette offentlig_flagg, men lar det stå som sikkerhet.  */
with siste_periode as (
  select max(b.rapport_periode) periode
  from {{ source('dt_kodeverk', 'dim_versjon') }}  b
  join {{ source('dt_kodeverk', 'publiseringstabell') }}  p on
      p.rapport_periode = b.rapport_periode and
      p.tabell_navn = 'IA_PARAM' and
      p.offentlig_dato < sysdate and
      b.tabell_navn = 'FAK_IA_SYKEFRAVAR'
),

/* Sykefravær, kvartalsstatistikk for virksomhet, én rad per virksomhet og varighet*/
sykefravar_statistikk_virksomhet_per_varighet as (
    select
    orgnr,
    arstall,
    kvartal,
    varighet,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(taptedv_gs) taptedv_gs,
    sum(antpers) antpers,
    rectype
from {{ source('syfra', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1 -- siste versjon med status 'GODKJENT' etter tidspunkt for pre-/offentliggjøring får flagg 1, eldre tabeller etter rekjøringer får flagg 0
--where dim.rapport_periode = (select periode from siste_periode) -- sjekker at siste periode er den samme som offentliggjort periode
where dim.rapport_periode <= (select periode from siste_periode) -- sjekker at siste periode ikke er større enn offentliggjort periode, vet henting av mer data tilbake i tid
 and dim.rapport_periode > (select periode - 500 from siste_periode) --henter data fra fem år tilbake, ikke aktuelt per september 2024
group by
    orgnr,
    arstall,
    kvartal,
    varighet,
    rectype
),

/* Sykefravær, kvartalsstatistikk for virksomhet, én rad per virksomhet*/
sykefravar_statistikk_virksomhet as (
  select
      orgnr,
      arstall,
      kvartal,
      sum(taptedv) taptedv,
      sum(muligedv) muligedv,
      sum(taptedv_gs) taptedv_gs,
      sum(antpers) antpers,
      rectype
  from sykefravar_statistikk_virksomhet_per_varighet
  group by
    orgnr,
    arstall,
    kvartal,
    rectype
),

/* Summerer tapte dagsverk per varighet som egne kolonner,
slik at granulariteten havner på virksomhet, og ikke virksomhet og varighet */
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

/* Joiner statistikk per virksomhet med aggregeringen på varighet
for full statistikk */
sykefravar_statistikk_virksomhet_med_varighet as (
  select
    s.orgnr,
    s.arstall,
    s.kvartal,
    round((s.taptedv/NULLIF(s.muligedv, 0)) * 100, 1) as prosent,
    s.taptedv,
    s.muligedv,
    s.taptedv_gs,
    s.antpers as antpers,
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
    cast(orgnr as varchar2(100)) as orgnr,
    cast(arstall as number(4)) as arstall, --ønsket som number av Team PIA
    cast(kvartal as number(1)) as kvartal, --ønsket som number av Team PIA
    cast( (case when prosent is null then 0 else prosent end ) as number(4,1)) as prosent, --ønsket med én desimal av Team PIA
    cast(taptedv as number(20,6)) as taptedv,
    cast(muligedv as number(20,6)) as muligedv,
    cast(taptedv_gs as number(20,6)) as taptedv_gs,
    cast(varighet_A as number(20,6)) as varighet_A,
    cast(varighet_B as number(20,6)) as varighet_B,
    cast(varighet_C as number(20,6)) as varighet_C,
    cast(varighet_D as number(20,6)) as varighet_D,
    cast(varighet_E as number(20,6)) as varighet_E,
    cast(varighet_F as number(20,6)) as varighet_F,
    cast(antpers as number(7,0)) as antpers,
    cast(rectype as varchar2(100)) as rectype
  from sykefravar_statistikk_virksomhet_med_varighet
)

select
  orgnr,
  arstall,
  kvartal,
  prosent,
  taptedv,
  muligedv,
  taptedv_gs,
  varighet_A,
  varighet_B,
  varighet_C,
  varighet_D,
  varighet_E,
  varighet_F,
  antpers,
  rectype
from final