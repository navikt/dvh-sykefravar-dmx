

WITH isdialogmote8 AS (
  SELECT *
  FROM {{ ref('stg_fk_isdialogmote_dm2') }}
),

info_minus_tid AS (
  SELECT * FROM
    (
      SELECT
        dialogmote_uuid,
        fk_person1,
        status_endring_type,
        virksomhetsnr,
        enhet_nr,
        sykmelder_flagg,
        arbeidstaker_flagg,
        arbeidsgiver_flagg,
        key_dmx
      FROM
        isdialogmote8
      GROUP BY
        dialogmote_uuid,
        fk_person1,
        status_endring_type,
        virksomhetsnr,
        enhet_nr,
        sykmelder_flagg,
        arbeidstaker_flagg,
        arbeidsgiver_flagg,
        key_dmx
    )
    pivot(COUNT(status_endring_type) for status_endring_type
     IN ('INNKALT' as INNKALT, 'NYTT_TID_STED' as NYTT_TID_STED, 'FERDIGSTILT' as FERDIGSTILT, 'AVLYST' as AVLYST))
),
status_tidspunkt AS (
  SELECT * FROM
    (
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
    pivot(MAX(status_endring_tidspunkt) for status_endring_type IN ('INNKALT' AS innkalt_tidspunkt, 'NYTT_TID_STED' AS nytt_tid_sted_tidspunkt, 'FERDIGSTILT' AS ferdigstilt_tidspunkt, 'AVLYST' AS avlyst_tidspunkt))
),

tid as (
    SELECT
    dialogmote_uuid,
    max(DIALOGMOTE_TIDSPUNKT) as nyeste_dialogmote,
    max(tilfelle_startdato) as nyeste_TILFELLE_STARTDATO
    FROM
    isdialogmote8
    GROUP BY
    dialogmote_uuid

),
FINAL AS (
  SELECT
    info_minus_tid.dialogmote_uuid,
    info_minus_tid.fk_person1,
    tid.nyeste_dialogmote,
    info_minus_tid.INNKALT,
    status_tidspunkt.innkalt_tidspunkt,
    info_minus_tid.NYTT_TID_STED,
    status_tidspunkt.nytt_tid_sted_tidspunkt,
    info_minus_tid.FERDIGSTILT,
    status_tidspunkt.ferdigstilt_tidspunkt,
    info_minus_tid.AVLYST,
    status_tidspunkt.avlyst_tidspunkt,
    info_minus_tid.VIRKSOMHETSNR,
    info_minus_tid.ENHET_NR,
    tid.nyeste_TILFELLE_STARTDATO,
    info_minus_tid.SYKMELDER_FLAGG,
    info_minus_tid.ARBEIDSTAKER_FLAGG,
    info_minus_tid.ARBEIDSGIVER_FLAGG,
    info_minus_tid.key_dmx
  FROM
    info_minus_tid
    left JOIN
    status_tidspunkt
    on info_minus_tid.dialogmote_uuid = status_tidspunkt.dialogmote_uuid
    left join
    tid
    on info_minus_tid.dialogmote_uuid = tid.dialogmote_uuid
)

SELECT * FROM FINAL where ferdigstilt = 1

--count(distinct(DIALOGMOTE_UUID))
-- 887401ee-88ca-45b9-ac5c-7076f2fa2988
-- 77c473a6-e8a4-4e18-8366-07cf3ab17d4f