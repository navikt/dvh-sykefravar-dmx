WITH vedtak AS (
  SELECT * FROM {{ source('sykp', 'tmp__raw_vedtak') }}
),

final AS (
  SELECT dokumenter.*, vedtak.kafka_hash AS fk_stg_tmp__raw_vedtak
  FROM   vedtak vedtak, json_table (
         vedtak.kafka_message, '$' columns (
           nested dokumenter[*]
           columns (
             dokumentId,
             type
       ) ) ) dokumenter
)

SELECT * FROM final
