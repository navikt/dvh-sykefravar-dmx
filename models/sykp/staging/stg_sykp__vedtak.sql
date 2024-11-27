/* Daglig last av sykepengevedtak.
-- Nye rader legges til med 'append'.
-- Kjøres med flagg --full-refresh for å opprette tabellen på nytt. */

with src_vedtak as (
  select
    json_value(vedtak.kafka_message,'$.fødselsnummer') as fnr,
    JSON_VALUE(vedtak.kafka_message,'$.organisasjonsnummer') as organisasjonsnummer,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.fom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_fom_dato,
    CAST(TO_TIMESTAMP_TZ(JSON_VALUE(vedtak.kafka_message,'$.tom'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AS timestamp) AS soknad_tom_dato,
    JSON_VALUE(vedtak.KAFKA_MESSAGE, '$.utbetalingId') as utbetaling_id,
    json_query(vedtak.KAFKA_MESSAGE, '$.dokumenter') as dokumenter,
    vedtak.kafka_topic,
    vedtak.kafka_partisjon,
    vedtak.kafka_offset,
    vedtak.kafka_mottatt_dato,
    vedtak.kildesystem,
    sysdate as oppdatert_dato,
    sysdate as lastet_dato
  from {{ source('dvh_sykp', 'raw_vedtak') }} vedtak
  where kafka_mottatt_dato < trunc(sysdate)
  and nvl(json_value(vedtak.kafka_message,'$.event'), 'vedtak_ok') <> 'vedtak_annullert'

  {% if is_incremental() %}
    and
        not exists (
        select 1
        from {{ this }} old
        where vedtak.kafka_partisjon = old.kafka_partisjon
          and vedtak.kafka_offset = old.kafka_offset -- legger til rad dersom kombinasjonen av partisjon og offset ikke allerede finnes i datasettet
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

vedtak_med_fk_person1 as (
  select
    vedtak.*,
    person.fk_person1 as pasient_fk_person1
  from src_vedtak vedtak
  inner join dvh_person_ident person on -- ekskluderer personer som ikke har treff i person-tabell
    vedtak.fnr = person.off_id
    and vedtak.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
    and person.skjermet_kode not in (6, 7)
),

-- Hentar ut alle dokument, både sykmeldingar, søknader og inntektsmeldingar
dokument as (
    select
        f.pasient_fk_person1,
        f.kafka_partisjon,
        f.kafka_offset,
        jt.dokument_id as dokument_id,
        jt.vedtak_type as vedtak_type
    from vedtak_med_fk_person1 f
    inner join json_table(
        f.dokumenter format json
        , '$[*]' columns(
            dokument_id varchar2(255) path '$.dokumentId'
            , vedtak_type varchar2(255) path '$.type'
        )
    ) jt on 1 = 1
), -- select dokument.* from dokument where vedtak_type in ('Sykmelding', 'Søknad');

-- Legg til alle sykmelding_id'ar og søknads-id'ar som finst på eit vedtak
stg_vedtak as (
  select
    v.*,
    soknad.dokument_id as soknad_id,
    sykmelding.dokument_id as sykmelding_id
  from vedtak_med_fk_person1 v
  left outer join dokument soknad
    on v.pasient_fk_person1 = soknad.pasient_fk_person1
    and v.kafka_partisjon = soknad.kafka_partisjon
    and v.kafka_offset = soknad.kafka_offset
    and soknad.vedtak_type = 'Søknad'
  left outer join dokument sykmelding
    on v.pasient_fk_person1 = sykmelding.pasient_fk_person1
    and v.kafka_partisjon = sykmelding.kafka_partisjon
    and v.kafka_offset = sykmelding.kafka_offset
    and sykmelding.vedtak_type = 'Sykmelding'
),   -- select * from stg_vedtak

pre_final as (
  select
    cast( pasient_fk_person1 as number(38,0) ) as pasient_fk_person1,
    cast( organisasjonsnummer as varchar2(9 char) ) as organisasjonsnummer,
    cast( soknad_fom_dato as date ) as soknad_fom_dato,
    cast( soknad_tom_dato as date ) as soknad_tom_dato,
    cast( soknad_id as varchar2(100) ) as soknad_id,
    cast( sykmelding_id as varchar2(100) ) as sykmelding_id,
    cast( utbetaling_id as varchar2(100) ) as utbetaling_id,
    cast( kafka_topic as varchar2(100) ) as kafka_topic,
    cast( kafka_partisjon as number(38,0) ) as kafka_partisjon,
    cast( kafka_offset as number(38,0) ) as kafka_offset,
    cast( kafka_mottatt_dato as date ) as kafka_mottatt_dato,
    cast( lastet_dato as date ) as lastet_dato,
    cast( oppdatert_dato as date ) as oppdatert_dato,
    cast( kildesystem as varchar2(10) ) as kildesystem
  from stg_vedtak
  ),  -- select * from pre_final

-- Begranser antall rader ved å kun velge den med min(sykmelding_id) og min(søknad_id.
-- Vi vil ha berre ei rad per kafka-melding, og plukkar første sykmelding_id og søknad_id fra meldingen)
  final as (
    select
    pasient_fk_person1,
    organisasjonsnummer,
    soknad_fom_dato,
    soknad_tom_dato,
    min(soknad_id) as soknad_id,
    min(sykmelding_id) as sykmelding_id,
    utbetaling_id,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    oppdatert_dato,
    kildesystem
  from pre_final
  group by     pasient_fk_person1,
    organisasjonsnummer,
    soknad_fom_dato,
    soknad_tom_dato,
    utbetaling_id,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    oppdatert_dato,
    kildesystem
  ) select * from final

