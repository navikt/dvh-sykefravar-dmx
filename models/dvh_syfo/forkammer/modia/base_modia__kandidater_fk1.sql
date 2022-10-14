{{config(
    materialized='table'
)}}

WITH kandidater_base AS (
  SELECT * FROM {{ ref('base_modia__kandidater') }}
),
dim_off_id AS (
    SELECT * FROM {{ref('felles_dt_person__dvh_person_ident_off_id') }}
),
final as
(
    SELECT
    kandidater_uuid,
    createdAt,
    kandidat_flagg,
    arsak,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem,
    dim_off_id.fk_person1 as fk_person1
    FROM kandidater_base
    LEFT JOIN dim_off_id
    ON kandidater_base.person_ident = dim_off_id.off_id
    where kandidater_base.kafka_mottatt_dato BETWEEN dim_off_id.gyldig_fra_dato AND dim_off_id.gyldig_til_dato
)

SELECT * FROM final