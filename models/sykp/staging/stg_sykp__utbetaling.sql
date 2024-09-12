 /* Daglig last av sykepengeutbetalingar.
-- Events som tas med: utbetaling_utbetalt.
-- Følgjande events blir dermed ikkje med: utbetaling_uten_utbetaling og utbetaling_annullert.
-- Nye rader legges til med 'append'.
-- Kjøres med flagg --full-refresh for å opprette tabellen på nytt. */

with src_utbetaling as (
  select
    json_value(utbetaling.kafka_message,'$.event') as utbetaling_event,
    JSON_VALUE(utbetaling.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    json_value(utbetaling.kafka_message,'$.fødselsnummer') as fnr,
    JSON_VALUE(utbetaling.kafka_message,'$.organisasjonsnummer') as organisasjonsnummer,
    JSON_VALUE(utbetaling.kafka_message,'$.forbrukteSykedager')  as forbrukteSykedager,
    JSON_VALUE(utbetaling.kafka_message,'$.gjenståendeSykedager') as gjenstaendeSykedager,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(utbetaling.kafka_message,'$.foreløpigBeregnetSluttPåSykepenger'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) as maksdato,
    JSON_VALUE(utbetaling.kafka_message,'$.type') as utbetaling_type,
    utbetaling.kafka_topic,
    utbetaling.kafka_partisjon,
    utbetaling.kafka_offset,
    utbetaling.kafka_mottatt_dato,
    utbetaling.kildesystem,
    utbetaling.lastet_dato as oppdatert_dato,
    utbetaling.lastet_dato
  from {{ source('sykp', 'raw_utbetaling') }} utbetaling
  where json_value(utbetaling.kafka_message,'$.event') = 'utbetaling_utbetalt'
  and kafka_mottatt_dato < trunc(sysdate)
),

dvh_person_ident as (
  select off_id,
         fk_person1,
         skjermet_kode,
         gyldig_fra_dato,
         gyldig_til_dato
   from  {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }}
),

utbetaling_med_fk_person1 as (
  select
    utbetaling.*,
    person.fk_person1 as pasient_fk_person1
  from src_utbetaling utbetaling
  inner join dvh_person_ident person on -- ekskluderer personer som ikke har treff i person-tabell
    utbetaling.fnr = person.off_id
    and utbetaling.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
    and person.skjermet_kode not in (6, 7)
),

final as (
  select
    cast( utbetaling_id as varchar2(100) ) as utbetaling_id,
    cast( pasient_fk_person1 as varchar2(100) ) as pasient_fk_person1,
    cast( organisasjonsnummer as varchar2(100) ) as organisasjonsnummer,
    cast( forbrukteSykedager as number(3,0) ) as forbrukte_sykedager,
    cast( gjenstaendeSykedager as number(3,0) ) as gjenstaende_sykedager,
    cast( maksdato as date ) as maksdato,
    cast( utbetaling_type as varchar2(100) ) as utbetaling_type,
    cast( kafka_topic as varchar2(100) ) as kafka_topic,
    cast( kafka_partisjon as number ) as kafka_partisjon,
    cast( kafka_offset as number ) as kafka_offset,
    cast( kafka_mottatt_dato as date ) as kafka_mottatt_dato,
    cast( lastet_dato as date ) as lastet_dato,
    cast( oppdatert_dato as date ) as oppdatert_dato,
    cast( kildesystem as varchar2(10) ) as kildesystem
  from utbetaling_med_fk_person1
  )
  select * from final