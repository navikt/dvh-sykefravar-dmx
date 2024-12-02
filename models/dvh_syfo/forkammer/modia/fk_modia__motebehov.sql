WITH motebehov AS (
  SELECT * FROM {{ source('modia', 'fk_motebehov') }}
)
,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__ident_off_id_til_fk_person1') }}
)
 ,join_fk_person_sm AS (
    SELECT
        motebehov.*,
        dvh_person_ident.fk_person1 as fk_person1_sm
        from motebehov
    LEFT JOIN dvh_person_ident
    ON
      motebehov.sm_fnr = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)
, join_fk_person1_2 as (
    select
    join_fk_person_sm.*,
    dvh_person_ident.fk_person1 as fk_person1_opprettet_av
    from join_fk_person_sm
  left join dvh_person_ident
    ON
      join_fk_person_sm.opprettet_av_fnr = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)
, final as (
  SELECT
    MOTEBEHOV_UUID,
    OPPRETTET_DATO,
    OPPRETTET_AV,
    VIRKSOMHETSNUMMER,
    HAR_MOTEBEHOV,
    TILDELT_ENHET,
    BEHANDLET_TIDSPUNKT,
    BEHANDLET_VEILEDER_IDENT,
    SKJEMATYPE,
    LASTET_DATO,
    KILDESYSTEM,
    FK_PERSON1_SM,
    FK_PERSON1_OPPRETTET_AV
  FROM join_fk_person1_2
)

select * from final