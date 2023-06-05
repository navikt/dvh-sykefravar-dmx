/* Et sykefraværstilfelle kan ha flere årsaker. Spesielt for 'AVVENT'.*/
/* For å få med vurderingar og unntak gjort innenfor en månad, kan vi bruke feltet SISTVURDERT,
som vil inneholde datoen for når vurderingen/hendelsen skjedde.
For statusene NY og AUTOMATISK_OPPFYLT er ikke SISTVURDERT utfylt, så de må vi plukke ut ved å bruke CREATEDAT. */

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
    LASTET_DATO,
    --sysdate as SYSDATE_DBT,
    OPPDATERT_DATO,
    --SISTVURDERT,
    CASE WHEN STATUS IN ('NY', 'AVVENT') and SISTVURDERT is null then CREATEDAT else SISTVURDERT END as SISTVURDERT,
    STATUS,
    STOPPUNKTAT,
    UPDATEDBY,
    CASE WHEN STATUS IN ('NY', 'AVVENT') then TO_CHAR(CREATEDAT, 'YYYYMM') else TO_CHAR(SISTVURDERT, 'YYYYMM') END as PERIODE
  FROM {{ ref("fk_modia__aktivitetskrav") }}
  where (
   SISTVURDERT < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
  and SISTVURDERT >= TO_DATE('{{var("last_mnd_start")}}','YYYY-MM-DD')
  ) OR
    (
      STATUS in ('NY', 'AVVENT') and CREATEDAT < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
      and CREATEDAT >= TO_DATE('{{var("last_mnd_start")}}','YYYY-MM-DD')
    )
),

sykefravar_tilfeller as(
  select
    FK_PERSON1,
    sykefravar_fra_dato
  from {{ ref("stg_fak_sykm_sykefravar_tilfelle") }}
),

/* WHERE clause sier at startdato for sykefraværstilfelle er tidligere enn dato for satt aktivitetskrav. */
sorterte_sykefravarstilfeller as (
  SELECT
    aktivitetskrav.*,
    sykefravar_tilfeller.sykefravar_fra_dato
  FROM aktivitetskrav
  LEFT JOIN sykefravar_tilfeller ON sykefravar_tilfeller.FK_PERSON1 = aktivitetskrav.FK_PERSON1
  where sykefravar_tilfeller.sykefravar_fra_dato < aktivitetskrav.CREATEDAT
  order by aktivitetskrav.FK_PERSON1,sykefravar_fra_dato,KAFKA_MOTTATT_DATO desc

),

/* Henter siste (latest) status fra sykefraværstilfelle med row-funksjon. */
siste_sykefravars_tilfeller as (
  SELECT
    sorterte_sykefravarstilfeller.*, sorterte_sykefravarstilfeller.sykefravar_fra_dato as siste_sykefravar_startdato,
    ROW_NUMBER() OVER (PARTITION BY FK_PERSON1, sykefravar_fra_dato, PERIODE ORDER BY KAFKA_MOTTATT_DATO desc) AS first_rownum
  FROM sorterte_sykefravarstilfeller

)

SELECT * FROM siste_sykefravars_tilfeller WHERE first_rownum=1