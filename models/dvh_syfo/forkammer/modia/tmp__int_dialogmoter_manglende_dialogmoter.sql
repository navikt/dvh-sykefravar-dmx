WITH fk_dialogmote_dm2 AS (
  SELECT
    dialogmote_uuid,
    dialogmote_tidspunkt,
    status_endring_type,
    status_endring_tidspunkt,
    fk_person1,
    virksomhetsnr,
    enhet_nr,
    nav_ident,
    tilfelle_startdato,
    arbeidstaker_flagg,
    arbeidsgiver_flagg,
    sykmelder_flagg,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  FROM {{ source('dmx_pox_dialogmote', 'FK_ISDIALOGMOTE_DM2') }}
  WHERE kafka_offset < 1953
)

, final AS (
  SELECT * FROM fk_dialogmote_dm2
)

SELECT * FROM final
