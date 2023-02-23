WITH motebehov_2 AS (
  SELECT * FROM {{ ref('fk_modia__motebehov') }}
)
,motebehov_innles AS (
    SELECT * FROM {{ref('fk_person_oversikt_status_base') }}
)
,ny_motebehov  AS (
    SELECT motebehov_2.*,
        motebehov_innles.tildelt_enhet as tildelt_enhet_ny
        from motebehov_2
    LEFT JOIN motebehov_innles
    ON
      motebehov_2.sm_fnr = motebehov_innles.fnr
),
 final AS (
   select
      MOTEBEHOV_UUID,OPPRETTET_DATO,
        OPPRETTET_AV,
        AKTOER_ID,
        VIRKSOMHETSNUMMER,
        HAR_MOTEBEHOV,
        BEHANDLET_TIDSPUNKT,
        BEHANDLET_VEILEDER_IDENT,
        SKJEMATYPE,
        SM_FNR,
        OPPRETTET_AV_FNR,
        LASTET_DATO,
        KILDESYSTEM,
        tildelt_enhet_ny as tildelt_enhet
        from ny_motebehov
 )

SELECT * FROM final