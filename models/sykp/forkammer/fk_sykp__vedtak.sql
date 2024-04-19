with vedtak as (
   select *  from {{ source('sykp', 'raw_vedtak_ny') }}
),
 person as (
  select * from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
 ),

vedtak_bygg AS ( select
    person.fk_person1 as pasient_fk_person1,
    JSON_VALUE(vedtak.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    JSON_VALUE(vedtak.kafka_message,'$.organisasjonsnummer') as organisasjonsnummer,
    CAST(TO_TIMESTAMP_TZ(vedtak.kafka_message.fom, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS soknad_fom_dato,
    CAST(TO_TIMESTAMP_TZ(vedtak.kafka_message.fom, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS soknad_tom_dato,
    json_value(vedtak.kafka_message,'$.doumenter.dokumentId') as dokument_sykemelding_id,
    json_value(vedtak.kafka_message,'$.doumenter.soknadId') as dokument_soknad_id,
    vedtak.kafka_topic,
    vedtak.kafka_partisjon,
    vedtak.kafka_offset,
    vedtak.kafka_mottatt_dato,
    vedtak.lastet_dato,
    vedtak.kildesystem
  from vedtak   
  inner join  person
          on person.off_id = json_value(vedtak.kafka_message,'$.fødselsnummer')
        and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY')
        and person.skjermet_kode not in (6, 7)
        WHERE json_value(vedtak.kafka_message,'$.utbetalingId') is not null


),

final as (
  select
        pasient_fk_person1,
        utbetaling_id,
        soknad_fom_dato,
        soknad_tom_dato,
        organisasjonsnummer,
        dokument_sykemelding_id,
        dokument_soknad_id,
        kafka_topic,
        kafka_partisjon,
        kafka_offset,
        kafka_mottatt_dato,
        lastet_dato,
        kildesystem
  from vedtak_bygg
)

select * from final