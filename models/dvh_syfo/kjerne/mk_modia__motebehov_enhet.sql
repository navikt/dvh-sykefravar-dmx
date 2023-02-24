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
      where motebehov_innles.tildelt_enhet is not null
)
,del1_med_nye_enheter AS (
 select ny_motebehov.*
  from ny_motebehov
  where ny_motebehov.tildelt_enhet is not null
)
,del2_tar_med_null AS (
  SELECT ny_motebehov.*,
        motebehov_2.tildelt_enhet as tildelt_enhet_null
        from ny_motebehov
    LEFT JOIN motebehov_2
    ON
      ny_motebehov.sm_fnr= motebehov_2.sm_fnr
    where ny_motebehov.tildelt_enhet is null
)
,total_med_null_og_opprinnelig  AS (
  select MOTEBEHOV_UUID,
        OPPRETTET_DATO,
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
        tildelt_enhet_ny as endelig_enhet
  from del1_med_nye_enheter
    union
  select MOTEBEHOV_UUID,
        OPPRETTET_DATO,
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
        tildelt_enhet_null as endelig_enhet
    from del2_tar_med_null
)
 ,final AS (
   select
      MOTEBEHOV_UUID,
      OPPRETTET_DATO,
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
        endelig_enhet as tildelt_enhet
        from total_med_null_og_opprinnelig
 )

SELECT * FROM final