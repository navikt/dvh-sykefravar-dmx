WITH motebehov_inn AS (
  SELECT * FROM {{ ref('mk_motebehov__prepp') }}
),
motebehov_enhet AS (
    SELECT * FROM {{ref('fk_person_oversikt_status') }}
),
final  AS (
    SELECT
        motebehov_inn.*,
        motebehov_enhet.tildelt_enhet as enhet_v1
        from motebehov_inn
    LEFT JOIN motebehov_enhet
    ON
      motebehov_inn.fk_person1 = motebehov_enhet.fk_person1
)

select * from final