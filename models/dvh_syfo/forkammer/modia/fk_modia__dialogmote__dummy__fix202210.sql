WITH source AS (
  -- TODO
  SELECT
    'AA' as dialogmote_uuid,
    TO_TIMESTAMP_TZ('2022-09-02 13', 'YYYY-MM-DD HH24') AT TIME ZONE 'CET' AS dialogmote_tidspunkt,
    'INNKALT' as status_endring_type,
    TO_TIMESTAMP_TZ('2022-09-02 09', 'YYYY-MM-DD HH24') AT TIME ZONE 'CET' AS status_endring_tidspunkt,
    2 as fk_person1,
    null as virksomhetsnr,
    0806 as enhet_nr,
    null as nav_ident,
    TO_TIMESTAMP_TZ('2022-07-01', 'YYYY-MM-DD') AT TIME ZONE 'CET' AS tilfelle_startdato,
    1 AS arbeidstaker_flagg,
    1 AS arbeidsgiver_flagg,
    1 AS sykmelder_flagg
  FROM DUAL
  UNION
  SELECT
      'AA' as dialogmote_uuid,
      TO_TIMESTAMP_TZ('2022-09-02 13', 'YYYY-MM-DD HH24') AT TIME ZONE 'CET' AS dialogmote_tidspunkt,
      'FERDIGSTILT' as status_endring_type,
      TO_TIMESTAMP_TZ('2022-09-02 14', 'YYYY-MM-DD HH24') AT TIME ZONE 'CET' AS status_endring_tidspunkt,
      2 AS fk_person1,
      null as virksomhetsnr,
      0806 as enhet_nr,
      null as nav_ident,
      TO_TIMESTAMP_TZ('2022-07-01', 'YYYY-MM-DD') AT TIME ZONE 'CET' AS tilfelle_startdato,
      1 AS arbeidstaker_flagg,
      1 AS arbeidsgiver_flagg,
      1 AS sykmelder_flagg
  FROM DUAL
)

SELECT * FROM source
