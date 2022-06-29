WITH vedtak AS (
  SELECT
    kafka_message AS record,
    kafka_hash
  FROM {{ source('sykp', 'tmp__raw_vedtak') }}
),

final AS (
  SELECT
    vedtak.kafka_hash AS id,
    vedtak.record.fødselsnummer AS fodselsnummer,
    vedtak.record.aktørId AS akorid,
    vedtak.record.organisasjonsnummer,
    to_date(vedtak.record.fom, 'YYYY-MM-DD') AS fom,
    to_date(vedtak.record.tom, 'YYYY-MM-DD') AS tom,
    to_date(vedtak.record.skjæringstidspunkt, 'YYYY-MM-DD') AS skjaeringstidspunkt,
    to_number(vedtak.record.inntekt) AS inntekt,
    to_number(vedtak.record.sykepengegrunnlag) AS sykepengegrunnlag,
    to_number(vedtak.record.grunnlagForSykepengegrunnlag) AS grunnlagForSykepengegrunnlag,
    vedtak.record.begrensning,
    vedtak.record.utbetalingId,
    to_timestamp(vedtak.record.vedtakFattetTidspunkt, 'YYYY-MM-DD"T"HH24:MI:SS.FF') AS vedtakFattetTidspunkt,
    vedtak.record.dokumenter
  FROM vedtak vedtak
)

SELECT * FROM final
