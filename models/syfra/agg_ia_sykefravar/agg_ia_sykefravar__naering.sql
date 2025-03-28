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

/* Sykefravær, kvartalsstatistikk for næring (2 siffer), én rad per næring og varighet*/
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
    and dim.offentlig_flagg = 1 -- siste versjon med status 'GODKJENT' etter tidspunkt for pre-/offentliggjøring får flagg 1, eldre tabeller etter rekjøringer får flagg 0
where dim.rapport_periode = (select periode from siste_periode) --sjekker at siste periode er den samme som offentliggjort periode
--where dim.rapport_periode <= (select periode from siste_periode) --sjekker at siste periode ikke er større enn offentliggjort periode, vet henting av mer data tilbake i tid
  --and dim.rapport_periode > (select periode - 100 from siste_periode) --henter data fra 1 år tilbake
and fak.rectype = 2 -- Filtrerer for kun rectype 2: VIRKSOMHET (B-nummer), tilsvarende offisiell sykefraværsstatistikk
group by
    naring,
    arstall,
    kvartal,
    varighet
),

/* Sykefravær, kvartalsstatistikk for næring (2 siffer), én rad per næring*/
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

/* Summerer tapte dagsverk per varighet som egne kolonner,
slik at granulariteten havner på næring, og ikke næring og varighet */
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

/* Joiner statistikk per næring med aggregeringen på varighet
for full statistikk */
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
    cast(arstall as number(4)) as arstall, --ønsket som number av Team PIA
    cast(kvartal as number(1)) as kvartal, --ønsket som number av Team PIA
    cast(round(taptedv/muligedv * 100, 1) as number(4,1)) as prosent, --ønsket med én desimal av Team PIA
    cast(taptedv as number(20,6)) as taptedv,
    cast(muligedv as number(20,6)) as muligedv,
    cast(taptedv_gs as number(20,6)) as taptedv_gs,
    cast(varighet_A as number(20,6)) as varighet_A,
    cast(varighet_B as number(20,6)) as varighet_B,
    cast(varighet_C as number(20,6)) as varighet_C,
    cast(varighet_D as number(20,6)) as varighet_D,
    cast(varighet_E as number(20,6)) as varighet_E,
    cast(varighet_F as number(20,6)) as varighet_F,
    cast(antpers as number(7,0)) as antpers
  from sykefravar_statistikk_naering_med_varighet
)

select
  naring,
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
  antpers
from final