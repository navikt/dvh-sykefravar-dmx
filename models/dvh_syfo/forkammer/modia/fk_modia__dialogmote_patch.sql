/*bytter ut tilfellestartdato som har blitt feil fra kilden*/

with dialogmote_forkammer as
(SELECT * FROM {{ref('fk_modia__dialogmote')}})

, dialogmote_patch as (
  select dialogmote_forkammer.*,
  case
  when kilde_uuid = '92097f17-1e02-4449-8eaf-d5a50d05220e' and hendelse in ('FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-06-01','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = 'c7745c33-fdff-42f8-a6dc-5d3dc4aed983' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-01-06','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra arena hendelse*/
  when kilde_uuid = 'b31aef9f-afaa-4d8e-9f31-d9d7d1bb3819' and hendelse in ('FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-08-28','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = '57c2abe2-062d-45bd-b51d-898e063cd13b' and hendelse in ('FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-09-08', 'YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = '5b3881ac-e5da-4d57-a4e5-5eeb90bf645a' and hendelse in ('INNKALT', 'FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-03-20','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = '159d7f6e-4ecf-487d-bf9e-1c9a8fb89e0d' and hendelse in ('FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-02-21','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = '9358c264-6871-4bad-b62b-a3245b52530e' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then TO_TIMESTAMP_TZ('2021-12-17','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = '4dfdc657-5879-40fc-a096-75d7562298c2' and hendelse in ('FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-08-18','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen modia hendelse*/
  when kilde_uuid = 'a2e8cd59-9eb4-44c7-bf4a-feb1da44312f' and hendelse in ('FERDIGSTILT', 'INNKALT') then TO_TIMESTAMP_TZ('2022-01-29','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra arena oppfolging*/
  when kilde_uuid = 'd6ed14e0-a98f-44fb-a114-88448b372b16' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-09-12','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra arena oppfolging*/
  when kilde_UUid = '812a477c-2543-427f-8cdb-fbef59f36123' and hendelse in ('FERDIGSTILT','INNKALT') then TO_TIMESTAMP_TZ('2022-02-28','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra arena/infotrygd*/
  when kilde_uuid = '3b5e4b16-3576-49c4-ba44-2393848e3fc5' and hendelse in ('INNKALT','NYTT_TID_STED','FERDIGSTILT') then TO_TIMESTAMP_TZ('2022-08-12','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra infotrygd/arena, fant ikke noe i sykm*/
  when kilde_uuid = '1662ee9f-5186-4c5b-a43b-bd0c926d8d19' and hendelse in ('FERDIGSTILT') then to_timestamp_tz('2022-09-14','YYYY-MM-DD') at TIME ZONE 'CET' /*hentet fra annen dialogmote hendelse*/
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
    person_ident,
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
