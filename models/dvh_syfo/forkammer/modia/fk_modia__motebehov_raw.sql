WITH motebehov AS (
  SELECT * FROM {{ source('modia', 'fk_motebehov_sky') }}
),

final as (
  select
    motebehov_uuid,
    opprettet_dato,
    opprettet_av,
    virksomhetsnummer,
    har_motebehov,
    tildelt_enhet,
    behandlet_tidspunkt,
    behandlet_veileder_ident,
    skjematype,
    sm_fnr,
    opprettet_av_fnr,
    lastet_dato,
    kildesystem
  from motebehov
)
select * from final