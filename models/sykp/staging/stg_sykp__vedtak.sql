with src_vedtak as (
  select
    person.fk_person1 as pasient_fk_person1,
    JSON_VALUE(vedtak.kafka_message,'$.organisasjonsnummer') as organisasjonsnummer,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_fom_dato,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_tom_dato,
    JSON_VALUE(vedtak.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
--    substr(json_query(vedtak.kafka_message, '$.dokumenter'), instr(json_query(vedtak.kafka_message, '$.dokumenter'), 'Søknad') - 46, 36) as soknad_id,
--    substr(json_query(vedtak.kafka_message, '$.dokumenter'), instr(json_query(vedtak.kafka_message, '$.dokumenter'), 'Sykmelding') - 46, 36) as sykmelding_id,
 --   soknad.dokument_id,
 --   sykmelding.dokument_id,
    vedtak.kafka_topic,
    vedtak.kafka_partisjon,
    vedtak.kafka_offset,
    vedtak.kafka_mottatt_dato,
    vedtak.kildesystem,
    vedtak.lastet_dato as oppdatert_dato,
    vedtak.lastet_dato
  from {{ source('dvh_sykp', 'raw_vedtak') }} vedtak
  inner join dt_person.ident_off_id_til_fk_person1 person
          on person.off_id = json_value(vedtak.kafka_message,'$.fødselsnummer')
        and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY')
        and person.skjermet_kode not in (6, 7),
      ),

  forste_utpakking as (
    select
        person.fk_person1 as pasient_fk_person1,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_fom_dato,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_tom_dato,
        json_query(vedtak.KAFKA_MESSAGE, '$.dokumenter') as dokumenter
    from raw_vedtak vedtak
    inner join dt_person.ident_off_id_til_fk_person1 person
          on person.off_id = json_value(vedtak.kafka_message,'$.fødselsnummer')
        and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY')
        and person.skjermet_kode not in (6, 7)
),-- select * from forste_utpakking;

dokument as (
    select
        f.pasient_fk_person1,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_fom_dato,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_tom_dato,
      , jt.dokument_id as dokument_id
      , jt.vedtak_type as vedtak_type
    from forste_utpakking f
    inner join json_table(
        f.dokumenter format json
        , '$[*]' columns(
            dokument_id varchar2(255) path '$.dokumentId'
            , vedtak_type varchar2(255) path '$.type'
        )
    ) jt on 1 = 1
), -- select andre_utpakking.* from andre_utpakking where vedtak_type in ('Sykmelding', 'Søknad');

stg_vedtak as (
  select
    pasient_fk_person1,
    organisasjonsnummer,
    v.soknad_fom_dato,
    v.soknad_tom_dato,
    utbetaling_id,
--    substr(json_query(vedtak.kafka_message, '$.dokumenter'), instr(json_query(vedtak.kafka_message, '$.dokumenter'), 'Søknad') - 46, 36) as soknad_id,
--    substr(json_query(vedtak.kafka_message, '$.dokumenter'), instr(json_query(vedtak.kafka_message, '$.dokumenter'), 'Sykmelding') - 46, 36) as sykmelding_id,
    soknad.dokument_id,
    sykmelding.dokument_id,
    vedtak.kafka_topic,
    vedtak.kafka_partisjon,
    vedtak.kafka_offset,
    vedtak.kafka_mottatt_dato,
    vedtak.kildesystem,
    vedtak.oppdatert_dato,
    vedtak.lastet_dato
  from src_vedtak v
  join dokument soknad
    on v.pasient_fk_person1 = soknad.pasient_fk_person1
    and v.soknad_fom_dato = soknad.soknad_fom_dato
    and v.soknad_tom_dato = soknad.soknad_tom_dato
    and soknad.vedtak_type = 'Soknad'
  join dokument sykmelding
    on v.pasient_fk_person1 = sykmelding.pasient_fk_person1
    and v.soknad_fom_dato = sykmelding.soknad_fom_dato
    and v.soknad_tom_dato = sykmelding.soknad_tom_dato
    and soknad.vedtak_type = 'Sykmelding'
)

select * from stg_vedtak

