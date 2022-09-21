WITH kandidater AS (
  SELECT * FROM {{ source('modia', 'raw_isdialogmotekandidat') }}
)

, final AS (
  SELECT
    kandidater.kafka_message.uuid as kandidater_uuid,
    TO_TIMESTAMP(kandidater.kafka_message.createdAt, 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm') AT TIME ZONE 'CET' AS createdAt,
    kandidater.kafka_message.personIdentNumber as person_ident,
    DECODE(kandidater.kafka_message.kandidat, 'true', 1, 'false', 0) AS kandidat_flagg,
    kandidater.kafka_message.arsak as arsak,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM kandidater kandidater
)

SELECT * FROM final





