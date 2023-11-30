{{
  config ( post_hook="grant READ ON {{ this }} to DVH_SYK_DBT" )
}}

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
  from source_fk_sporbar_utbetaling_kafka_offset
)

select * from final
