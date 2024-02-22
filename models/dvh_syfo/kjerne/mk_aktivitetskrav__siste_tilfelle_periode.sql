/* Et sykefraværstilfelle kan ha flere årsaker. Spesielt for 'AVVENT'.*/
/* For å få med vurderingar og unntak gjort innenfor en måned, kan vi bruke feltet SISTVURDERT,
som vil inneholde datoen for når vurderingen/hendelsen skjedde.
For statusene NY, AUTOMATISK_OPPFYLT og NY_VURDERING er ikke SISTVURDERT utfylt, så de må vi plukke ut ved å bruke CREATEDAT.
AUTOMATISK_OPPFYLT som siste status ekskluderes da person ikke vurderes.
Status LUKKET ikke håndtert. Havner som null i periode ved telling */


WITH aktivitetskrav_last as (
  SELECT
    kafka_mottatt_dato,
    arsaker,
    arsaker1,
    arsaker2,
    createdat,
    fk_person1,
    lastet_dato,
    sysdate as lastet_dato_dbt,
    case when status in ('NY', 'AUTOMATISK_OPPFYLT', 'NY_VURDERING') and sistvurdert is null then createdat else sistvurdert end as sistvurdert,
    status,
    stoppunktat,
    case when status IN ('NY', 'AUTOMATISK_OPPFYLT', 'NY_VURDERING') then TO_CHAR(createdat, 'YYYYMM') else TO_CHAR(sistvurdert, 'YYYYMM') end as periode,
    tilfelle_startdato,
    oppdatert_dato
  FROM {{ ref('mk_aktivitetskrav__join_sykefravar_tilfelle') }}

  where (
    sistvurdert < TO_DATE('{{var("slutt_dato_last")}}','YYYY-MM-DD')
    and sistvurdert >= TO_DATE('{{var("start_dato_last")}}','YYYY-MM-DD')
    ) or
    (
    status in ('NY', 'AUTOMATISK_OPPFYLT', 'NY_VURDERING') and createdat < TO_DATE('{{var("slutt_dato_last")}}','YYYY-MM-DD')
    and createdat >= TO_DATE('{{var("start_dato_last")}}','YYYY-MM-DD')
    )

),


/* Grupperer sykefraværstilfeller innenfor periode med row-funksjon.
WHERE indikerer at startdato for sykefraværstilfelle er tidligere enn dato for satt aktivitetskrav.*/
siste_tilfelle_i_periode as (
  select
    aktivitetskrav_last.*,
    ROW_NUMBER() over (partition by fk_person1, periode order by tilfelle_startdato desc, kafka_mottatt_dato desc) as rangerte_rader
  from aktivitetskrav_last
  where aktivitetskrav_last.tilfelle_startdato < aktivitetskrav_last.createdat and
   aktivitetskrav_last.tilfelle_startdato < aktivitetskrav_last.stoppunktat
),

/* Henter siste (latest) status fra sykefraværstilfeller innenfor periode (rangerte_rader = 1). */
final as (
  select
    fk_person1,
    periode,
    status,
    oppdatert_dato,
    arsaker,
    arsaker1,
    arsaker2,
    sistvurdert,
    stoppunktat,
    tilfelle_startdato as siste_tilfelle_startdato,
    lastet_dato,
    lastet_dato_dbt
  from siste_tilfelle_i_periode
  where rangerte_rader=1 and status != 'AUTOMATISK_OPPFYLT'
)

select * from final

-- mappe for aktivitetskrav
-- lage mk_aktivitetskrav__inkr for å sette filter
-- mk_aktivitetskrav__ekstra_kolonner /__andre_flagg / __gyldige_rader / __beriket
-- mk_aktivitetskrav__surr_key ? Hvor bør denne logikken ligge? Det er et view, må den ligge i torg?
-- bytte ut sistvurdert/createdat med 'hendelse_dato'
