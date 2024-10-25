 /* Daglig last av annulleringar av sykepengeutbetalingar.
-- Events som tas med: utbetaling_annullert.
-- Følgjande events blir dermed ikkje med: utbetaling_uten_utbetaling og utbetaling_utbetalt.
-- Nye rader legges til med 'append'.
-- Kjøres med flagg --full-refresh for å opprette tabellen på nytt. */

with src_annullering as (
  select
    json_value(annullering.kafka_message,'$.event') as utbetaling_event,
    JSON_VALUE(annullering.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    json_value(annullering.kafka_message,'$.fødselsnummer') as fnr,
    JSON_VALUE(annullering.kafka_message,'$.organisasjonsnummer') as organisasjonsnummer,
    annullering.kafka_topic,
    annullering.kafka_partisjon,
    annullering.kafka_offset,
    annullering.kafka_mottatt_dato,
    annullering.kildesystem,
    sysdate as lastet_dato,
    sysdate as oppdatert_dato
  from {{ source('sykp', 'raw_utbetaling') }} annullering
  where json_value(utbetaling.kafka_message,'$.event') = 'utbetaling_annullert'
  and kafka_mottatt_dato < trunc(sysdate)

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

annullering_med_fk_person1 as (
  select
    annullering.*,
    person.fk_person1 as pasient_fk_person1
  from src_annullering annullering
  inner join dvh_person_ident person on -- ekskluderer personer som ikke har treff i person-tabell
    annullering.fnr = person.off_id
    and annullering.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
    and person.skjermet_kode not in (6, 7)
),

final as (
  select
    cast( utbetaling_id as varchar2(100 char) ) as utbetaling_id,
    cast( pasient_fk_person1 as number(38,0) ) as pasient_fk_person1,
    cast( organisasjonsnummer as varchar2(9 char) ) as organisasjonsnummer,
    cast( kafka_topic as varchar2(100 char) ) as kafka_topic,
    cast( kafka_partisjon as number(38,0) ) as kafka_partisjon,
    cast( kafka_offset as number(38,0) ) as kafka_offset,
    cast( kafka_mottatt_dato as date ) as kafka_mottatt_dato,
    cast( lastet_dato as date ) as lastet_dato,
    cast( oppdatert_dato as date ) as oppdatert_dato,
    cast( kildesystem as varchar2(10 char) ) as kildesystem
  from annullering_med_fk_person1
  )
  select * from final