{{ config(
    grants={"read": "dvh_oppgave_app"}
) }}

WITH tilfeller AS (
  SELECT * FROM {{ ref('fk_dt_sensitv__fak_sykm_sykefravar_tilfelle') }}
  WHERE sykefravar_til_dato >= trunc(sysdate - 7)
)

,periode AS (
  SELECT * FROM {{ ref('fk_fk_sensitiv__sykm_periode') }}
  WHERE sykmelding_tom >= trunc(sysdate - 160)
)

,max_tom AS (
  select pasient_fk_person1, sykmelding_tom, gradering,
  rank() over (partition by pasient_fk_person1 order by sykmelding_tom desc, lastet_dato desc) rank
  from periode
)

,gradering AS (
  select pasient_fk_person1, sykmelding_tom, gradering
  from max_tom
  where rank = 1
)

,final AS (
  SELECT
    fk_person1
    ,sykefravar_fra_dato
    ,sykefravar_til_dato
    ,gradering
    ,TRUNC(oppdatert_dato) AS oppdatert_dato
  FROM
    tilfeller
  LEFT JOIN gradering ON
    fk_person1 = pasient_fk_person1
)

SELECT * FROM final
