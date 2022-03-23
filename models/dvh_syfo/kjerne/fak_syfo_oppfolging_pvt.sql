WITH isdialogmote8 AS (
  SELECT *
  FROM {{ ref('stg_fk_isdialogmote_dm2') }}
),

info_minus_tid AS (
  SELECT * FROM (
    SELECT
      dialogmote_uuid,
      fk_person1,
      status_endring_type,
      virksomhetsnr,
      enhet_nr,
      arbeidstaker_flagg,
      arbeidsgiver_flagg
    FROM
      isdialogmote8
    GROUP BY
      dialogmote_uuid,
      fk_person1,
      status_endring_type,
      virksomhetsnr,
      enhet_nr,
      arbeidstaker_flagg,
      arbeidsgiver_flagg
  )
  PIVOT ( -- noqa
    COUNT(status_endring_type)
    FOR status_endring_type IN (
      'INNKALT' AS innkalt,
      'NYTT_TID_STED' AS nytt_tid_sted,
      'FERDIGSTILT' AS ferdigstilt,
      'AVLYST' AS avlyst
    )
  )
),

status_tidspunkt AS (
  SELECT * FROM (
    SELECT
      dialogmote_uuid,
      status_endring_type,
      status_endring_tidspunkt
    FROM
      isdialogmote8
    GROUP BY
      dialogmote_uuid,
      status_endring_type,
      status_endring_tidspunkt
  )
  PIVOT ( --noqa
    MAX(status_endring_tidspunkt)
    FOR status_endring_type IN (
      'INNKALT' AS innkalt_tidspunkt,
      'NYTT_TID_STED' AS nytt_tid_sted_tidspunkt,
      'FERDIGSTILT' AS ferdigstilt_tidspunkt,
      'AVLYST' AS avlyst_tidspunkt
    )
  )
),

tid AS (
  SELECT
    dialogmote_uuid,
    max(dialogmote_tidspunkt) AS nyeste_dialogmote,
    max(tilfelle_startdato) AS nyeste_tilfelle_startdato
  FROM
    isdialogmote8
  GROUP BY
    dialogmote_uuid
),

final AS (
  SELECT
    info_minus_tid.dialogmote_uuid,
    info_minus_tid.fk_person1,
    tid.nyeste_dialogmote,
    info_minus_tid.innkalt,
    status_tidspunkt.innkalt_tidspunkt,
    info_minus_tid.nytt_tid_sted,
    status_tidspunkt.nytt_tid_sted_tidspunkt,
    info_minus_tid.ferdigstilt,
    status_tidspunkt.ferdigstilt_tidspunkt,
    info_minus_tid.avlyst,
    status_tidspunkt.avlyst_tidspunkt,
    info_minus_tid.virksomhetsnr,
    info_minus_tid.enhet_nr,
    tid.nyeste_tilfelle_startdato,
    info_minus_tid.arbeidstaker_flagg,
    info_minus_tid.arbeidsgiver_flagg
  FROM
    info_minus_tid
  LEFT JOIN
    status_tidspunkt
    ON info_minus_tid.dialogmote_uuid = status_tidspunkt.dialogmote_uuid
  LEFT JOIN
    tid
    ON info_minus_tid.dialogmote_uuid = tid.dialogmote_uuid
)

SELECT * FROM final
