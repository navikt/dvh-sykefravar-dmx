WITH dialogmote AS (
  SELECT * FROM {{ source('modia', 'raw_isdialogmote') }}
)

, final AS (
  SELECT
    dialogmote.kafka_message.dialogmoteUuid as dialogmote_uuid,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.dialogmoteTidspunkt, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS dialogmote_tidspunkt,
    dialogmote.kafka_message.statusEndringType as status_endring_type,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.statusEndringTidspunkt, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS status_endring_tidspunkt,
    dialogmote.kafka_message.personIdent as person_ident,
    dialogmote.kafka_message.virksomhetsnummer as virksomhetsnr,
    dialogmote.kafka_message.enhetNr as enhet_nr,
    dialogmote.kafka_message.navIdent as nav_ident,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.tilfelleStartdato, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS tilfelle_startdato,
    DECODE(dialogmote.kafka_message.arbeidstaker, 'true', 1, 'false', 0) AS arbeidstaker_flagg,
    DECODE(dialogmote.kafka_message.arbeidsgiver, 'true', 1, 'false', 0) AS arbeidsgiver_flagg,
    DECODE(dialogmote.kafka_message.sykmelder, 'true', 1, 'false', 0) AS sykmelder_flagg,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM dialogmote dialogmote
)

SELECT * FROM final