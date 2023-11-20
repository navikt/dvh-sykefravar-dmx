{% macro lest_fk_sporbar_utbetaling(kilde_tabell) %}

SELECT
    *
FROM
    (
        SELECT
            kafka_topic,
            kafka_partisjon,
            kafka_offset,
            LEAD(
                kafka_offset
            ) OVER(PARTITION BY
                kafka_topic,
                kafka_partisjon
                ORDER BY kafka_offset
            ) neste
        FROM
            {{ kilde_tabell }}
        WHERE
            kafka_mottatt_dato > sysdate - 14
    )
WHERE
    neste - kafka_offset > 2

{% endmacro %}
