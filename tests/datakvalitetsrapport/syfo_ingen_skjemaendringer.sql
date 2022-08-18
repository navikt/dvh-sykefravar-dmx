WITH forrige_kafka_message AS (
    SELECT kafka_message
    FROM {{ source('dmx_pox_dialogmote', 'raw_isdialogmote') }}
    WHERE trunc(lastet_dato) < trunc(sysdate)
    ORDER BY lastet_dato DESC
    FETCH FIRST ROW ONLY
)

, dagens_kafka_message AS (
    SELECT kafka_message
    FROM {{ source('dmx_pox_dialogmote', 'raw_isdialogmote') }}
    WHERE trunc(lastet_dato) like trunc(sysdate)
    ORDER BY lastet_dato DESC
    FETCH FIRST ROW ONLY
)

, forrige_json_dataguide AS (
    SELECT
      TREAT(JSON_DATAGUIDE(kafka_message) AS JSON) AS json_dataguide
    FROM forrige_kafka_message
)

, dagens_json_dataguide AS (
    SELECT
        TREAT(JSON_DATAGUIDE(kafka_message) AS JSON) AS json_dataguide
    FROM dagens_kafka_message
)

, forrige_schema AS (
    SELECT
        forrige_json_dataguide.json_dataguide."o:path" AS jschema
    FROM forrige_json_dataguide forrige_json_dataguide
)

, dagens_schema AS (
    SELECT
        dagens_json_dataguide.json_dataguide."o:path" AS jschema
    FROM dagens_json_dataguide dagens_json_dataguide
)

, sammenlign_schemaer AS (
    SELECT dagens_schema.jschema AS dagens_schema, forrige_schema.jschema AS forrige_schema
    FROM dagens_schema, forrige_schema
    WHERE dagens_schema.jschema != forrige_schema.jschema
)

SELECT * FROM sammenlign_schemaer
