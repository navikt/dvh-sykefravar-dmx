WITH dialogmote AS (
  SELECT kafka_message AS record
  FROM {{ source('dmx_pox_dialogmote', 'raw_isdialogmote') }}
),
final AS (
  SELECT
    dialogmote.record.dialogmoteUuid,
    TO_TIMESTAMP_TZ(dialogmote.record.dialogmoteTidspunkt, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS dialogmoteTidspunkt,
    dialogmote.record.statusEndringType,
    TO_TIMESTAMP_TZ(dialogmote.record.statusEndringTidspunkt, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS statusEndringTidspunkt,
    dialogmote.record.personIdent,
    dialogmote.record.virksomhetsnummer,
    dialogmote.record.enhetNr,
    dialogmote.record.navIdent,
    TO_TIMESTAMP_TZ(dialogmote.record.tilfelleStartdato, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS tilfelleStartdato,
    DECODE(dialogmote.record.arbeidstaker, 'true', 1, 'false', 0) AS arbeidstaker,
    DECODE(dialogmote.record.arbeidsgiver, 'true', 1, 'false', 0) AS arbeidsgiver,
    DECODE(dialogmote.record.sykmelder, 'true', 1, 'false', 0) AS sykmelder
  FROM dialogmote dialogmote
)
SELECT * FROM final
