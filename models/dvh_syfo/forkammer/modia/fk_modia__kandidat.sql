WITH kandidater AS (
  SELECT * FROM {{ source('modia', 'raw_isdialogmotekandidat') }}
)
, final AS (
  SELECT
    kandidater.kafka_message.uuid as kilde_uuid,
    CAST(TO_TIMESTAMP(kandidater.kafka_message.createdAt, 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm') at TIME ZONE 'CET' as timestamp) AS hendelse_tidspunkt,
    person.fk_person1 as fk_person1,
    DECODE(kandidater.kafka_message.kandidat, 'true', 1, 'false', 0) AS kandidat_flagg,
    kandidater.kafka_message.arsak as hendelse,
    kandidater.kafka_message.unntakArsak as unntakarsak,
    TO_DATE(kandidater.kafka_message.tilfelleStart, 'YYYY-MM-DD') as tilfelle_startdato,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM kandidater kandidater
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
          on person.off_id = kandidater.kafka_message.personIdentNumber
         and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
         and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
)
SELECT * FROM final
