WITH aktivitetskrav AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav') }}
)

, final AS (
  SELECT
    aktivitetskrav.kafka_message.Uuid as kilde_uuid,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.aktivitetskravTidspunkt, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS aktivitetskrav_tidspunkt,
   TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.sistVurdert, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS sistVurdert,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.stoppunktAt, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS stoppunktAt,
    aktivitetskrav.kafka_message.status as status,
     aktivitetskrav.kafka_message.arsak as arsak,
    aktivitetskrav.kafka_message.beskrivelse as beskrivelse,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.createdAt, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS createdAt,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_key,
    kafka_timestamp,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  FROM aktivitetskrav
)

SELECT * FROM final
