with utbetaling as (
   select *  from {{ source('sykp', 'raw_utbetaling_ny') }}
),
 person as (
  select * from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
 ),

utbetaling_bygg AS ( select
    JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    JSON_VALUE(utbetaling.kafka_message,'$.oppdragsType') as oppdragstype,
    json_value(utbetaling.kafka_message,'$.fagsystemId') as fagsystem_id,
    JSON_VALUE(utbetaling.kafka_message,'$.organisasjonsnummer') as mottaker_orgnummer,
    person.fk_person1 as mottaker_fk_person1,
    JSON_VALUE(utbetaling.kafka_message,'$.nettoBeløp')  as netto_belop,
    JSON_VALUE(utbetaling.kafka_message,'$.stønadsDager') as stonadsdager,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS tom,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS fom,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS utbetalt_tom_dato,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS utbetalt_fom_dato,
    JSON_VALUE(utbetaling.kafka_message,'$.dagSats') as dagSats,
    JSON_VALUE(utbetaling.kafka_message,'$.grad') as grad,
    json_value(utbetaling.kafka_message,'$.totalBeløp') as totalBeløp,
    --oppdatert dato
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
  select
        utbetaling_id,
        oppdragstype,
        fagsystem_id,
        mottaker_orgnummer,
        mottaker_fk_person1,
        netto_belop,
        stonadsdager,
        tom,
        fom,
        utbetalt_tom_dato,
        utbetalt_fom_dato,
        dagSats,
        grad,
        totalBeløp,
        lastet_dato,
        kildesystem
  from utbetaling_bygg
)

select * from final