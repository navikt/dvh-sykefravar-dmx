
WITH vedtak AS (
  SELECT * FROM {{ source('sykp', 'raw_vedtak') }}
)

, final AS (
  SELECT
    ' ' as id,
    ' ' as PASIENT_FK_PERSON1,
    vedtak.kafka_message.aktørId as AKTOR_ID,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM vedtak vedtak
)

SELECT * FROM final




