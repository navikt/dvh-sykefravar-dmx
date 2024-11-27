 /* Daglig last av utbetalingsoppdrag for sykepengar, der utbetalingen skjer til arbeidstakar/person.
-- Kun events av type utbetaling_utbetalt tas med.
-- Nye rader legges til med 'append'.
-- Kjøres med flagg --full-refresh for å opprette tabellen på nytt. */


with src_person_oppdrag as (
  select
    JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    JSON_VALUE(utbetaling.kafka_message,'$.personOppdrag.mottaker') as mottaker_fnr,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagsystemId') as fagsystem_id,
    json_value(utbetaling.kafka_message,'$.personOppdrag.fagområde') as fagomraade,
    jt_linjer.totalbelop as totalbelop,
    jt_linjer.stonadsdager as stonadsdager,
    jt_linjer.grad as grad,
    jt_linjer.dagsats as dagsats,
    jt_linjer.utbetalt_fom_dato as utbetalt_fom_dato,
    jt_linjer.utbetalt_tom_dato as utbetalt_tom_dato,
    utbetaling.kildesystem,
    utbetaling.kafka_mottatt_dato,
    utbetaling.kafka_partisjon,
    utbetaling.kafka_offset,
    sysdate as oppdatert_dato,
    sysdate as lastet_dato
  from {{ source('dvh_sykp', 'raw_utbetaling') }} utbetaling
    CROSS JOIN JSON_TABLE(utbetaling.kafka_message, '$.personOppdrag.utbetalingslinjer[*]'
          COLUMNS (totalbelop        NUMBER(10,0) PATH '$.totalbeløp',
                   stonadsdager      NUMBER(3,0)  PATH '$.stønadsdager',
                   dagsats           NUMBER(10,0) PATH '$.dagsats',
                   utbetalt_fom_dato DATE PATH '$.fom',
                   utbetalt_tom_dato DATE PATH '$.tom',
                   grad NUMBER(3,0)  PATH '$.grad')) jt_linjer
    where kafka_mottatt_dato < trunc(sysdate)
    and json_value(utbetaling.kafka_message,'$.event') = 'utbetaling_utbetalt'

 {% if is_incremental() %}
    and
        not exists (
        select 1
        from {{ this }} old
        where utbetaling.kafka_partisjon = old.kafka_partisjon
          and utbetaling.kafka_offset = old.kafka_offset -- legger til rad dersom kombinasjonen av partisjon og offset ikke allerede finnes i datasettet
      )
  {% endif %}

  ),

  dvh_person_ident as (
  select off_id,
         fk_person1,
         skjermet_kode,
         gyldig_fra_dato,
         gyldig_til_dato
   from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
  ),

  oppdrag_med_fk_person1 as (
  select
    oppdrag.*,
    person.fk_person1 as pasient_fk_person1
  from src_person_oppdrag oppdrag
  inner join dvh_person_ident person on -- ekskluderer personer som ikke har treff i person-tabell
    oppdrag.mottaker_fnr = person.off_id
    and oppdrag.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
    and person.skjermet_kode not in (6, 7)
  ),

  final as (
    select
      cast( utbetaling_id as varchar2(100 char) ) as utbetaling_id,
      cast( pasient_fk_person1 as varchar2(11 char) ) as mottaker_fk_person1,
      cast( fagsystem_id as varchar2(50 char) ) as fagsystem_id,
      cast( fagomraade as varchar2(50 char) ) as fagomraade,
      cast( totalbelop as number(10,0) ) as totalbelop,
      cast( stonadsdager as number(3,0) ) as stonadsdager,
      cast( grad as number(3,0) ) as grad,
      cast( dagsats as number(10,0) ) as dagsats,
      cast( utbetalt_fom_dato as date) as utbetalt_fom_dato,
      cast( utbetalt_tom_dato as date) as utbetalt_tom_dato,
      cast( kafka_mottatt_dato as date) as kafka_mottatt_dato,
      cast( kafka_partisjon as number(38,0) ) as kafka_partisjon,
      cast( kafka_offset as number(38,0) ) as kafka_offset,
      cast( lastet_dato as date ) as lastet_dato,
      cast( oppdatert_dato as date ) as oppdatert_dato,
      cast( kildesystem as varchar2(10 char) ) as kildesystem
  from  oppdrag_med_fk_person1
  )
  select * from final


