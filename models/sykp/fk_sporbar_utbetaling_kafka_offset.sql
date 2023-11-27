

with source_fk_sporbar_utbetaling_kafka_offset as (
  SELECT
            kafka_topic,
            kafka_partisjon,
            kafka_offset,
            kafka_mottatt_dato
        FROM
            {{ source('sykp', 'fk_sporbar_utbetaling') }}
        UNION
                SELECT
            kafka_topic,
            kafka_partisjon,
            kafka_offset,
            kafka_mottatt_dato
        FROM
            {{ source('sykp', 'fk_sporbar_annullering') }}
          ),

final as (
  select    kafka_topic,
            kafka_partisjon,
            kafka_offset,
            kafka_mottatt_dato
  from source_fk_sporbar_utbetaling_kafka_offsett
)

select * from final
