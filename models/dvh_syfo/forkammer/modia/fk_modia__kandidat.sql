{{ config(
    materialized='table'
)}}

WITH kandidater AS (
  SELECT * FROM {{ source('modia', 'raw_isdialogmotekandidat') }}
)
, utpakking AS (
  SELECT
    kandidater.kafka_message.uuid as kilde_uuid,
    TO_TIMESTAMP_TZ(kandidater.kafka_message.createdAt, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZR') at TIME ZONE 'Europe/Oslo' AS hendelse_tidspunkt,
    person.fk_person1 as fk_person1,
    DECODE(kandidater.kafka_message.kandidat, 'true', 1, 'false', 0) AS kandidat_flagg,
    kandidater.kafka_message.arsak as hendelse,
    kandidater.kafka_message.unntakArsak as unntakarsak,
    TO_DATE(kandidater.kafka_message.tilfelleStart, 'YYYY-MM-DD') as tilfelle_startdato,
    kandidater.kafka_message.unntakVeilederident as nav_ident,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM kandidater kandidater
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
          on person.off_id = kandidater.kafka_message.personIdentNumber
         --and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
         and kandidater.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
         and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
),

final as (
  select
    kilde_uuid,
    cast(hendelse_tidspunkt as date) as hendelse_tidspunkt,
    fk_person1,
    kandidat_flagg,
    hendelse,
    unntakarsak,
    tilfelle_startdato,
    nav_ident,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    sysdate as lastet_dato,
    sysdate as oppdatert_dato,
    kildesystem
  from utpakking
)
SELECT * FROM final
