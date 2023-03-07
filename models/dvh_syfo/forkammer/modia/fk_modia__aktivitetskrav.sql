
WITH aktivitetskrav AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav') }}
)

, final AS (
  SELECT
    aktivitetskrav.kafka_message.uuid as kilde_uuid,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.createdAt, 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') AT TIME ZONE 'CET' as createdAt,
    aktivitetskrav.kafka_message.status as status,
    aktivitetskrav.kafka_message.beskrivelse as beskrivelse,
    aktivitetskrav.kafka_message.arsaker as arsaker,
    aktivitetskrav.kafka_message.updatedBy as updatedBy,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.stoppunktAt, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS stoppunktAt,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.sistVurdert, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS sistVurdert,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  FROM aktivitetskrav aktivitetskrav
)

SELECT * FROM final
