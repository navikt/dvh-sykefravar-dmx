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
    TO_CHAR(SISTVURDERT, 'YYYYMM') as PERIODE
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
    sykefravar_fra_dato as siste_sykefravar_startdato
  from {{ ref("stg_fak_sykm_sykefravar_tilfelle") }}
),

sorterte_sykefravarstilfeller as (
  SELECT
    aktivitetskrav.*,
    sykefravar_tilfeller.siste_sykefravar_startdato
  FROM aktivitetskrav
  LEFT JOIN sykefravar_tilfeller ON sykefravar_tilfeller.FK_PERSON1 = aktivitetskrav.FK_PERSON1
  where sykefravar_tilfeller.siste_sykefravar_startdato < aktivitetskrav.CREATEDAT
  order by aktivitetskrav.FK_PERSON1,SISTE_SYKEFRAVAR_STARTDATO,KAFKA_MOTTATT_DATO desc

),

siste_sykefravars_tilfeller as (
  SELECT
    sorterte_sykefravarstilfeller.*,
    ROW_NUMBER() OVER (PARTITION BY FK_PERSON1, SISTE_SYKEFRAVAR_STARTDATO, PERIODE ORDER BY KAFKA_MOTTATT_DATO desc) AS first_rownum
  FROM sorterte_sykefravarstilfeller

)

SELECT * FROM siste_sykefravars_tilfeller WHERE first_rownum=1