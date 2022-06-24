WITH raw_dialogmote AS (
  SELECT kafka_message
  FROM {{ source('dmx_pox_dialogmote', 'tmp__raw_isdialogmote') }}
),
final AS (
  SELECT
    d.kafka_message.dialogmoteUuid,
    d.kafka_message.dialogmoteTidspunkt,
    d.kafka_message.statusEndringType,
    d.kafka_message.statusEndringTidspunkt,
    d.kafka_message.personIdent,
    d.kafka_message.virksomhetsnummer,
    d.kafka_message.enhetNr,
    d.kafka_message.navIdent,
    d.kafka_message.tilfelleStartdato,
    d.kafka_message.arbeidstaker,
    d.kafka_message.arbeidsgiver,
    d.kafka_message.sykmelder
  FROM raw_dialogmote d
)
SELECT * FROM final
