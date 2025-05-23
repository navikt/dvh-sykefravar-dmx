/*bytter ut tilfellestartdato som har blitt feil fra kilden*/

with dialogmote_forkammer as
(SELECT * FROM {{ref('fk_modia__dialogmote')}})

, dialogmote_patch as (
  select dialogmote_forkammer.*,
  case
  when kilde_uuid = '92097f17-1e02-4449-8eaf-d5a50d05220e' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-06-01','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = 'c7745c33-fdff-42f8-a6dc-5d3dc4aed983' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-01-06','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra arena hendelse*/
  when kilde_uuid = 'b31aef9f-afaa-4d8e-9f31-d9d7d1bb3819' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-08-28','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = '57c2abe2-062d-45bd-b51d-898e063cd13b' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-09-08', 'YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = '5b3881ac-e5da-4d57-a4e5-5eeb90bf645a' and hendelse in ('INNKALT', 'FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-03-20','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = '159d7f6e-4ecf-487d-bf9e-1c9a8fb89e0d' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-02-21','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = '9358c264-6871-4bad-b62b-a3245b52530e' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2021-12-17','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = '4dfdc657-5879-40fc-a096-75d7562298c2' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-08-18','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen modia hendelse*/
  when kilde_uuid = 'a2e8cd59-9eb4-44c7-bf4a-feb1da44312f' and hendelse in ('FERDIGSTILT', 'INNKALT') then CAST(TO_TIMESTAMP_TZ('2022-01-29','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra arena oppfolging*/
  when kilde_uuid = 'd6ed14e0-a98f-44fb-a114-88448b372b16' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-09-12','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra arena oppfolging*/
  when kilde_UUid = '812a477c-2543-427f-8cdb-fbef59f36123' and hendelse in ('FERDIGSTILT','INNKALT') then CAST(TO_TIMESTAMP_TZ('2022-02-28','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra arena/infotrygd*/
  when kilde_uuid = '3b5e4b16-3576-49c4-ba44-2393848e3fc5' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-08-12','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra infotrygd/arena, fant ikke noe i sykm*/
  when kilde_uuid = '1662ee9f-5186-4c5b-a43b-bd0c926d8d19' and hendelse in ('FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2022-09-14','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen dialogmote hendelse*/
  when kilde_uuid = '7352a6f3-d7e2-4676-a2fe-de0c55f59f68' and hendelse in ('FERDIGSTILT','INNKALT') then  CAST(TO_TIMESTAMP_TZ('2023-01-23','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra sf_oppfolging*/
  when kilde_uuid = '7574b760-cef4-44a7-9c21-463055fd6a36' and hendelse in ('FERDIGSTILT','INNKALT') then  CAST(TO_TIMESTAMP_TZ('2023-10-09','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra sf_oppfolging og sykm_sykmelding*/
  when kilde_uuid = 'a5a7e0c1-a7b1-428c-b868-0b747cd51009' and hendelse in ('FERDIGSTILT','INNKALT', 'NYTT_TID_STED') then  CAST(TO_TIMESTAMP_TZ('2024-04-05','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra sykm_sykmelding*/
  when kilde_uuid = '9f0795df-dd18-4c9a-8720-8eef63fa03f4' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2024-09-18','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen dialogmote hendelse*/
  when kilde_uuid = 'ed082aab-3881-44c6-b386-1cfca0e94199' and hendelse in ('INNKALT','FERDIGSTILT') then CAST(TO_TIMESTAMP_TZ('2025-02-17','YYYY-MM-DD') at TIME ZONE 'CET' as timestamp) /*hentet fra annen dialogmote hendelse*/
  else tilfelle_startdato
  end
  tilfelle_startdato_patch
  from dialogmote_forkammer
)

, final AS (
  SELECT
    kilde_uuid,
    dialogmote_tidspunkt,
    hendelse,
    hendelse_tidspunkt,
    fk_person1,
    virksomhetsnr,
    enhet_nr,
    nav_ident,
    tilfelle_startdato_patch as tilfelle_startdato,
    arbeidstaker_flagg,
    arbeidsgiver_flagg,
    sykmelder_flagg,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM dialogmote_patch
)

SELECT * FROM final
