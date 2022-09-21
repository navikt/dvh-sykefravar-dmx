WITH kandidater AS (
  SELECT * FROM {{ source('modia', 'raw_isdialogmote') }}
)

, final AS (
  SELECT
    dialogmote.kafka_message.dialogmoteUuid as dialogmote_uuid,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM kandidater
)

SELECT * FROM final

/*
      kandidater.kafka_message.kandidaterUuid as kandidater_uuid,
   TO_TIMESTAMP_TZ(kandidater.kafka_message.createdAt, 'YYYY-MM-DD HH24:MI:SS:TZH:TZM') AT TIME ZONE 'CET' AS createdAt,
    kandidater.kafka_message.personIdent as person_ident,
    DECODE(kandidater.kafka_message.kandidat, 'true', 1, 'false', 0) AS kandidat_flagg,
    kandidater.kafka_message.arsak as arsak,
{"uuid": "60a259ee-ac5d-42fa-8d92-87b77788d961", "createdAt": "2022-06-30T01:37:00.122345206Z", "personIdentNumber": "04867799392", "kandidat": true, "arsak": "STOPPUNKT"}
create table RAW_ISkandidater
(
	KAFKA_HASH VARCHAR2(255 char),
	KAFKA_MESSAGE CLOB
		constraint RAW_ISDIALOGMOTEKANDIDAT_KAFKA_MESSAGE_JSON
			check (kafka_message is json),
	KAFKA_TOPIC VARCHAR2(255 char),
	KAFKA_PARTISJON NUMBER,
	KAFKA_OFFSET NUMBER,
	KAFKA_KEY VARCHAR2(255 char),
	KAFKA_TIMESTAMP NUMBER,
	KAFKA_MOTTATT_DATO DATE,
	OPPDATERT_DATO DATE,
	LASTET_DATO DATE,
	KILDESYSTEM VARCHAR2(50 char)
)
*/




