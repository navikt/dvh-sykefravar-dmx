WITH utbetaling AS (
    select
    utbetaling.kafka_message.utbetalingsid as utbetaling_id,
    utbetaling.kafka_message.hendelse as utbetaling_event,
    CAST(TO_TIMESTAMP_TZ(utbetaling.kafka_message.fom, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS fom,
    CAST(TO_TIMESTAMP_TZ(utbetaling.kafka_message.tom, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS tom,
    person.fk_person1 as pasient_fk_person1,
    utbetaling.kafka_message.organisasjonsnr as organisasjonsnr,
    utbetaling.kafka_message.forbrukteSykedager  as forbrukteSykedager,
    utbetaling.kafka_message.gjenstaendeSykedager as gjenstaendeSykedager,
    utbetaling.kafka_message.aktørid as aktor_id,
    utbetaling.kafka_topic,
    utbetaling.kafka_partisjon,
    utbetaling.kafka_offset,
    utbetaling.kafka_mottatt_dato,
    utbetaling.lastet_dato,
    utbetaling.kildesystem
  from {{ source('sykp', 'raw_utbetaling_ny') }} utbetaling where utbetaling.kafka_message.event = 'utbetaling_utbetalt'
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
          on person.off_id = utbetaling.kafka_message.fødselsnummer
        and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
        and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
),

final as (
  select utbetaling_id,
        utbetaling_event
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
  from utbetaling
)

select * from final