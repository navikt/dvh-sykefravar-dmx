{{ config(materialized='table') }}

WITH dialogmote AS (
  SELECT * FROM {{ ref("tmp__stg_isdialogmote") }}
),
dvh_person_ident AS (
  SELECT * FROM {{ source('dt_person', 'dvh_person_ident_off_id') }}
),
dialogmote_filtrert AS (
  SELECT *
  FROM dialogmote
  WHERE kafka_offset BETWEEN 26243 AND 26247
),
dialogmote_filtrert_join_dvh_person_ident AS (
  SELECT
    dialogmote_filtrert.*,
    dvh_person_ident.fk_person1
  FROM dialogmote_filtrert
  LEFT JOIN dvh_person_ident
  ON dialogmote_filtrert.person_ident = dvh_person_ident.off_id
  WHERE
    dialogmote_filtrert.kafka_mottatt_dato
      BETWEEN dvh_person_ident.gyldig_fra_dato
      AND dvh_person_ident.gyldig_til_dato
),
final AS (
  SELECT
    dialogmote_filtrert_join_dvh_person_ident.fk_person1,
    dialogmote_filtrert_join_dvh_person_ident.dialogmote_uuid,
    CAST(dialogmote_filtrert_join_dvh_person_ident.dialogmote_tidspunkt AS DATE) AS dialogmote_tidspunkt,
    dialogmote_filtrert_join_dvh_person_ident.status_endring_type,
    CAST(dialogmote_filtrert_join_dvh_person_ident.status_endring_tidspunkt AS DATE) AS status_endring_tidspunkt,
    dialogmote_filtrert_join_dvh_person_ident.virksomhetsnr,
    dialogmote_filtrert_join_dvh_person_ident.enhet_nr,
    dialogmote_filtrert_join_dvh_person_ident.nav_ident,
    CAST(dialogmote_filtrert_join_dvh_person_ident.tilfelle_startdato AS DATE) AS tilfelle_startdato,
    dialogmote_filtrert_join_dvh_person_ident.arbeidstaker_flagg,
    dialogmote_filtrert_join_dvh_person_ident.arbeidsgiver_flagg,
    dialogmote_filtrert_join_dvh_person_ident.sykmelder_flagg,
    dialogmote_filtrert_join_dvh_person_ident.kafka_topic,
    dialogmote_filtrert_join_dvh_person_ident.kafka_partisjon,
    dialogmote_filtrert_join_dvh_person_ident.kafka_offset,
    dialogmote_filtrert_join_dvh_person_ident.kafka_mottatt_dato,
    sysdate AS oppdatert_dato,
    dialogmote_filtrert_join_dvh_person_ident.lastet_dato,
    dialogmote_filtrert_join_dvh_person_ident.kildesystem
  FROM dialogmote_filtrert_join_dvh_person_ident
)
SELECT hibernate_sequence.nextval AS id, final.* FROM final
