
WITH motebehov as (
  SELECT * FROM {{ ref("fk_modia__motebehov") }}
)

,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__dvh_person_ident_off_id') }}
)

, join_fk_person_sm AS (
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
    dvh_person_ident.fk_person1 as fk_person1_behov
    from join_fk_person_sm
  left join dvh_person_ident
    ON
      join_fk_person_sm.opprettet_av_fnr = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)

,final AS (
  SELECT
    *
  FROM join_fk_person1_2
)

SELECT * FROM final
