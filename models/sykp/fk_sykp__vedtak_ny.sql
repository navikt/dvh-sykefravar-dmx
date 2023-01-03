
WITH vedtak AS (
  SELECT * FROM {{ source('sykp', 'raw_vedtak') }}
)

, final AS (
  SELECT
    ' ' as id,
    ' ' as PASIENT_FK_PERSON1,
    vedtak.kafka_message.aktørId as AKTOR_ID,
    vedtak.kafka_message.utbetalingId as UTBETALING_ID,
    vedtak.kafka_message.fom as SOKNAD_FOM_DATO,
    vedtak.kafka_message.tom as SOKNAD_TOM_DATO,
    vedtak.kafka_message.inntekt as INNTEKT,
    vedtak.kafka_message.organisasjonsnummer as ORGANISASJONSNUMMER,
    vedtak.kafka_message.skjæringstidspunkt as SKJAERINGSTIDSPUNKT,
    vedtak.kafka_message.sykepengegrunnlag as SYKEPENGEGRUNNLAG,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM vedtak vedtak
)

SELECT * FROM final




