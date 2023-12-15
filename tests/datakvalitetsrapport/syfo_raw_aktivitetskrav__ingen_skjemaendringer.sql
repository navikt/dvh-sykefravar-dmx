WITH kafka_meldinger AS (
    SELECT
        kafka_hash,
        kafka_message
    FROM {{ source('modia', 'raw_aktivitetskrav') }}
    WHERE lastet_dato > sysdate - 1 and kafka_message is not null
)

, json_dataguide AS (
    SELECT
        kafka_hash,
        TREAT(JSON_DATAGUIDE(kafka_message) AS JSON) AS json_dataguide
    FROM kafka_meldinger
    GROUP BY kafka_hash
)

, jschema AS (
    SELECT
        json_dataguide.json_dataguide."o:path" AS jschema
    FROM json_dataguide json_dataguide
    GROUP BY json_dataguide.json_dataguide."o:path"
)

, final AS (
    SELECT
        COUNT(*)
    FROM jschema
    HAVING COUNT(*) > 1
)

SELECT * FROM final
