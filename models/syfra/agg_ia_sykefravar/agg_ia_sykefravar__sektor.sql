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

/* Sykefravær, kvartalsstatistikk for sektor, én rad per sektor*/
sykefravar_statistikk_sektor as (
  select
    sektor,
    arstall,
    kvartal,
    sum(taptedv) taptedv,
    sum(muligedv) muligedv,
    sum(antpers) antpers
from {{ source('syfra', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1 -- siste versjon med status 'GODKJENT' etter tidspunkt for pre-/offentliggjøring får flagg 1, eldre tabeller etter rekjøringer får flagg 0
where dim.rapport_periode = (select periode from siste_periode) --sjekker at siste periode er den samme som offentliggjort periode
--where dim.rapport_periode <= (select periode from siste_periode) --sjekker at siste periode ikke er større enn offentliggjort periode, vet henting av mer data tilbake i tid
-- and dim.rapport_periode > (select periode - 500 from siste_periode) --henter data fra fem år tilbake, ikke aktuelt per september 2024
and fak.rectype = 2 -- Filtrerer for kun rectype 2: VIRKSOMHET (B-nummer), tilsvarende offisiell sykefraværsstatistikk
group by
    sektor,
    arstall,
    kvartal
),

final as (
  select
    cast(sektor as varchar2(100)) as sektor,
    cast(arstall as number(4)) as arstall, --ønsket som number av Team PIA
    cast(kvartal as number(1)) as kvartal, --ønsket som number av Team PIA
    cast(round(taptedv/muligedv * 100, 1) as number(4,1)) as prosent, --ønsket med én desimal av Team PIA
    cast(taptedv as number(20,6)) as taptedv,
    cast(muligedv as number(20,6)) as muligedv,
    cast(antpers as number(7,0)) as antpers
  from sykefravar_statistikk_sektor
)

select
  sektor,
  arstall,
  kvartal,
  prosent,
  taptedv,
  muligedv,
  antpers
from final

