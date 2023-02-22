WITH motebehov AS (
  SELECT * FROM {{ ref('fk_modia__motebehov') }}
),
motebehov_innles AS (
    SELECT * FROM {{ref('fk_person_oversikt_status') }}
),
final  AS (
    SELECT
        motebehov_2.*,
        motebehov_innles.tildelt_enhet as enhet_v1
        from motebehov_2
    LEFT JOIN motebehov_innles
    ON
      motebehov_2.fk_person1 = motebehov_innles.fk_person1
)

select * from final