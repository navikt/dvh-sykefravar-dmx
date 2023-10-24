/* Et sykefraværstilfelle kan ha flere årsaker. Spesielt for 'AVVENT'.*/
/* For å få med vurderingar og unntak gjort innenfor en månad, kan vi bruke feltet SISTVURDERT,
som vil inneholde datoen for når vurderingen/hendelsen skjedde.
For statusene NY og AUTOMATISK_OPPFYLT er ikke SISTVURDERT utfylt, så de må vi plukke ut ved å bruke CREATEDAT.
AUTOMATISK_OPPFYLT ekskluderes da person ikke vurderes.*/

{{
  config(
    materialized='incremental',
    incrmental_strategy = 'merge'
  )
}}


WITH aktivitetskrav as (
  SELECT
    ARSAKER,
    ARSAKER1,
    ARSAKER2,
    CREATEDAT,
    FK_PERSON1,
    KAFKA_MOTTATT_DATO,
    KAFKA_OFFSET,
    KAFKA_PARTISJON,
    KAFKA_TOPIC,
    KILDE_UUID,
    KILDESYSTEM,
    LASTET_DATO, -- Kafka
    --sysdate as SYSDATE_DBT,
    OPPDATERT_DATO,
    CASE WHEN STATUS IN ('NY') and SISTVURDERT is null then CREATEDAT else SISTVURDERT END as SISTVURDERT,
    STATUS,
    STOPPUNKTAT,
    UPDATEDBY,
    CASE WHEN STATUS IN ('NY') then TO_CHAR(CREATEDAT, 'YYYYMM') else TO_CHAR(SISTVURDERT, 'YYYYMM') END as PERIODE
  FROM {{ ref("fk_modia__aktivitetskrav") }}

  {% if is_incremental() %}
    where (
        SISTVURDERT < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
        and SISTVURDERT >= TO_DATE('{{var("last_mnd_start")}}','YYYY-MM-DD')
      ) OR
      (
        STATUS in ('NY') and CREATEDAT < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
        and CREATEDAT >= TO_DATE('{{var("last_mnd_start")}}','YYYY-MM-DD')
      )
  {% endif %}

),

sykefravar_tilfeller as(
  select
    FK_PERSON1,
    sykefravar_fra_dato
  from {{ ref("stg_fak_sykm_sykefravar_tilfelle") }}
),

sorterte_aktivitetskrav as (
  SELECT
    aktivitetskrav.*,
    sykefravar_tilfeller.sykefravar_fra_dato
  FROM aktivitetskrav
  LEFT JOIN sykefravar_tilfeller ON sykefravar_tilfeller.FK_PERSON1 = aktivitetskrav.FK_PERSON1
  order by aktivitetskrav.FK_PERSON1,sykefravar_fra_dato,KAFKA_MOTTATT_DATO desc
),

/* Sykefravær 'under behandling' vil ikke finnes, selv om aktivitetskravet gjør det.
Ønsker også få med aktivitetskravene som av en eller annen grunn har et sykefraværstart etter aktivitetskravet startet og stoppet.
Setter derfor sykefraværstart basert på frist 56 dager.
*/
inkludere_aktivitetskrav_uten_sykefravar_treff as (
  select
    sorterte_aktivitetskrav.*,
    case when (sykefravar_fra_dato is null) or (sykefravar_fra_dato > CREATEDAT) or (sykefravar_fra_dato > stoppunktat) then to_date(stoppunktat-56) else sykefravar_fra_dato end as sykefravar_start
  from sorterte_aktivitetskrav
),

/* Henter siste (latest) status fra sykefraværstilfelle med row-funksjon.
WHERE clause sier at startdato for sykefraværstilfelle er tidligere enn dato for satt aktivitetskrav.*/
siste_sykefravars_tilfeller as (
  SELECT
    inkludere_aktivitetskrav_uten_sykefravar_treff.*, inkludere_aktivitetskrav_uten_sykefravar_treff.sykefravar_start as siste_sykefravar_startdato,
    ROW_NUMBER() OVER (PARTITION BY FK_PERSON1, PERIODE ORDER BY sykefravar_start desc, KAFKA_MOTTATT_DATO desc) AS rangerte_rader
  FROM inkludere_aktivitetskrav_uten_sykefravar_treff
  where inkludere_aktivitetskrav_uten_sykefravar_treff.sykefravar_start < inkludere_aktivitetskrav_uten_sykefravar_treff.CREATEDAT and
   inkludere_aktivitetskrav_uten_sykefravar_treff.sykefravar_start < inkludere_aktivitetskrav_uten_sykefravar_treff.stoppunktat
)

SELECT * FROM siste_sykefravars_tilfeller WHERE rangerte_rader=1
