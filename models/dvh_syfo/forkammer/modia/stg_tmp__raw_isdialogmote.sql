WITH dialogmote AS (
  SELECT kafka_message AS record
  FROM {{ source('dmx_pox_dialogmote', 'tmp__raw_isdialogmote') }}
),
final AS (
  SELECT
    dialogmote.record.dialogmoteUuid,
    dialogmote.record.dialogmoteTidspunkt,
    dialogmote.record.statusEndringType,
    dialogmote.record.statusEndringTidspunkt,
    dialogmote.record.personIdent,
    dialogmote.record.virksomhetsnummer,
    dialogmote.record.enhetNr,
    dialogmote.record.navIdent,
    dialogmote.record.tilfelleStartdato,
    dialogmote.record.arbeidstaker,
    dialogmote.record.arbeidsgiver,
    dialogmote.record.sykmelder
  FROM dialogmote dialogmote
)
SELECT * FROM final
