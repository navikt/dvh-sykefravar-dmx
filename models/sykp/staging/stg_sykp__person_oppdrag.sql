 /* Daglig last av utbetalingsoppdrag for sykepengar, der utbetalingen skjer til arbeidstakar/person.
-- Kun events av type utbetaling_utbetalt tas med.
-- Nye rader legges til med 'append'.
-- Kjøres med flagg --full-refresh for å opprette tabellen på nytt. */

{{ config(
    materialized='table'
)}}

with src_person_oppdrag as (
  select
    JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    JSON_VALUE(utbetaling.kafka_message,'$.personOppdrag.mottaker') as mottaker_orgnummer,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagsystemId') as fagsystem_id,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagområde') as fagomraade,
    json_query(utbetaling.kafka_message, '$.personOppdrag.utbetalingslinjer') as utbetalingslinjer,
    jt_linjer.totalbelop as totalbelop,
    jt_linjer.stonadsdager as stonadsdager,
    jt_linjer.grad as grad,
    jt_linjer.dagsats as dagsats,
    jt_linjer.utbetalt_fom_dato as utbetalt_fom_dato,
    jt_linjer.utbetalt_tom_dato as utbetalt_tom_dato,
    utbetaling.kildesystem,
    utbetaling.lastet_dato as oppdatert_dato,
    utbetaling.lastet_dato
  from {{ source('sykp', 'raw_utbetaling') }} utbetaling
    CROSS JOIN JSON_TABLE(utbetaling.kafka_message, '$.personOppdrag.utbetalingslinjer[*]'
          COLUMNS (totalbelop        NUMBER(10,0) PATH '$.totalbelÃžp',
                   stonadsdager      NUMBER(3,0)  PATH '$.stÃžnadsdager',
                   dagsats           NUMBER(10,0) PATH '$.dagsats',
                   utbetalt_fom_dato DATE PATH '$.fom',
                   utbetalt_tom_dato DATE PATH '$.tom',
                   grad NUMBER(3,0)  PATH '$.grad')) jt_linjer
    where kafka_mottatt_dato < trunc(sysdate)
    and json_value(utbetaling.kafka_message,'$.event') = 'utbetaling_utbetalt'
  ),

  final as (
    select
      cast( utbetaling_id as varchar2(100 char) ) as utbetaling_id,
      cast( mottaker_orgnummer as varchar2(9 char) ) as mottaker_orgnummer,
      cast( fagsystem_id as varchar2(50 char) ) as fagsystem_id,
      cast( fagomraade as varchar2(50 char) ) as fagomraade,
      cast( utbetalingslinjer as varchar2(1000 char) ) as utbetalingslinjer,
      cast( totalbelop as number(10,0) ) as totalbelop,
      cast( stonadsdager as number(3,0) ) as stonadsdager,
      cast( grad as number(3,0) ) as grad,
      cast( dagsats as number(10,0) ) as dagsats,
      cast( utbetalt_fom_dato as date) as utbetalt_fom_dato,
      cast( utbetalt_tom_dato as date) as utbetalt_tom_dato,
      cast( lastet_dato as date ) as lastet_dato,
      cast( oppdatert_dato as date ) as oppdatert_dato,
      cast( kildesystem as varchar2(10) ) as kildesystem
  from  src_person_oppdrag
  )
  select * from final


