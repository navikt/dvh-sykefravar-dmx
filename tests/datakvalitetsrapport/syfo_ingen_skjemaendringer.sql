WITH forrige_kafka_message AS (
    SELECT kafka_message
    FROM {{ source('dmx_pox_dialogmote', 'raw_isdialogmote') }}
    WHERE trunc(lastet_dato) < trunc(sysdate)
    ORDER BY lastet_dato DESC
    FETCH FIRST ROW ONLY
), dagens_kafka_message AS (
    SELECT kafka_message
    FROM {{ source('dmx_pox_dialogmote', 'raw_isdialogmote') }}
    WHERE trunc(lastet_dato) like trunc(sysdate)
    ORDER BY lastet_dato DESC
    FETCH FIRST ROW ONLY
), forrige_schema AS (
    SELECT
        TO_CHAR(JSON_DATAGUIDE(kafka_message)) AS jschema
    FROM forrige_kafka_message
), dagens_schema AS (
    SELECT
        TO_CHAR(JSON_DATAGUIDE(kafka_message)) AS jschema
    FROM dagens_kafka_message
), sammenlign_schemaer AS (
    SELECT dagens_schema.jschema
    FROM dagens_schema, forrige_schema
    WHERE dagens_schema.jschema != forrige_schema.jschema
)
SELECT * FROM sammenlign_schemaer
