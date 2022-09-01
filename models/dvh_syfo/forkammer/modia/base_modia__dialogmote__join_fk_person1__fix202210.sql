WITH dialogmoter AS (
  SELECT * FROM {{ ref('base_modia__dialogmote') }}
)

, dvh_person AS (
  SELECT * FROM {{ ref('felles_dt_person__dvh_person_ident_off_id') }}
)

, final AS (
  SELECT
    dialogmote_uuid,
    dialogmote_tidspunkt,
    status_endring_type,
    status_endring_tidspunkt,
    dvh_person.fk_person1 AS fk_person1,
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
  FROM dialogmoter
  LEFT JOIN dvh_person
  ON dialogmoter.person_ident = dvh_person.off_id
  WHERE dialogmoter.kafka_mottatt_dato BETWEEN dvh_person.gyldig_fra_dato AND dvh_person.gyldig_til_dato
)

SELECT * FROM final
