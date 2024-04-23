with utbetaling as (
   select *  from {{ source('sykp', 'raw_utbetaling_ny') }}
),
 person as (
  select * from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
 ),

utbetaling_bygg AS ( select
    JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    'personOppdrag' as oppdragstype,
    JSON_VALUE(utbetaling.kafka_message,'$.organisasjonsnummer') as mottaker_orgnummer,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagsystemId') as fagsystem_id,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagområde') as fagomraade,
    person.fk_person1 as mottaker_fk_person1,
    JSON_VALUE(utbetaling.kafka_message,'$.personOppdrag.nettoBeløp')  as netto_belop,
    JSON_VALUE(utbetaling.kafka_message,'$.stønadsdager') as stonadsdager,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS tom,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS fom,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.oppdatert_dato'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS oppdatert_dato,
    jt_linjer.grad as grad,
    jt_linjer.dagsats as dagsats,
    jt_linjer.utbetalt_tom_dato as utbetalt_tom_dato,
    jt_linjer.utbetalt_fom_dato as utbetalt_fom_dato,
    utbetaling.lastet_dato,
    utbetaling.kildesystem
  from utbetaling
    CROSS JOIN JSON_TABLE(utbetaling.kafka_message, '$.personOppdrag.utbetalingslinjer[*]'
          COLUMNS (dagsats NUMBER(10,0) PATH '$.dagsats',
                    utbetalt_fom_dato DATE PATH '$.fom',
                    utbetalt_tom_dato DATE PATH '$.tom',
                    grad NUMBER(3,0) PATH '$.grad')) jt_linjer

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
        mottaker_orgnummer,
        mottaker_fk_person1,
        fagsystem_id,
        fagomraade,
        netto_belop,
        stonadsdager,
        tom,
        fom,
        oppdatert_dato,
        grad,
        dagsats,
        utbetalt_tom_dato,
        utbetalt_fom_dato,
        lastet_dato,
        kildesystem

  from utbetaling_bygg
)

select * from final