

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
            dvh_sykp.fk_sporbar_utbetaling_kafka_offset
        WHERE
            kafka_mottatt_dato > sysdate - 14
    )
WHERE
    neste - kafka_offset > 1
