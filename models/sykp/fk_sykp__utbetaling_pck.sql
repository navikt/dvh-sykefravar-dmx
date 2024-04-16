with utbetaling as (
   select *  from {{ source('sykp', 'raw_utbetaling_ny') }}
),
 person as (
  select * from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
 ),

utbetaling_bygg AS ( select
     JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    json_value(utbetaling.kafka_message,'$.event') as utbetaling_event,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS tom,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS fom,
    person.fk_person1 as pasient_fk_person1,
    JSON_VALUE(utbetaling.kafka_message,'$.organisasjonsnr') as organisasjonsnr,
    JSON_VALUE(utbetaling.kafka_message,'$.forbrukteSykedager')  as forbrukteSykedager,
    JSON_VALUE(utbetaling.kafka_message,'$.gjenstaendeSykedager') as gjenstaendeSykedager,
    JSON_VALUE(utbetaling.kafka_message,'$.aktørId') as aktor_id,
    utbetaling.kafka_topic,
    utbetaling.kafka_partisjon,
    utbetaling.kafka_offset,
    utbetaling.kafka_mottatt_dato,
    utbetaling.lastet_dato,
    utbetaling.kildesystem
  from utbetaling
  inner join  person
          on person.off_id = json_value(utbetaling.kafka_message,'$.fødselsnummer')
        and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY')
        and person.skjermet_kode not in (6, 7)
      where json_value(utbetaling.kafka_message,'$.event') = 'utbetaling_utbetalt'

),

final as (
  select utbetaling_id,
        utbetaling_event,
        fom,
        tom,
        pasient_fk_person1,
        organisasjonsnr,
        forbrukteSykedager,
        gjenstaendeSykedager,
        aktor_id,
        kafka_topic,
        kafka_partisjon,
        kafka_offset,
        kafka_mottatt_dato,
        lastet_dato,
        kildesystem
  from utbetaling_bygg
)

select * from final