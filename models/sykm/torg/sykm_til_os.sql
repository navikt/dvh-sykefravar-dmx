-- NB! Pass på at komponentskjemaet har grant option til underliggande tabellar
{{ config(
    grants={"read": "dvh_oppgave_app"}
) }}

WITH tilfeller AS (
  SELECT * FROM {{ ref('fk_dt_sensitv__fak_sykm_sykefravar_tilfelle') }}
  WHERE sykefravar_til_dato between trunc(sysdate - 14) and trunc(sysdate + 365)
)

,periode AS (
  SELECT pasient_fk_person1,
         sykmelding_tom,
         gradering,
         lastet_dato FROM {{ ref('fk_fk_sensitiv__sykm_periode') }}
  WHERE sykmelding_tom >= trunc(sysdate - 14)
    union
  SELECT fk_person1 as pasient_fk_person1,
         sykmeldt_til_dato as sykmelding_tom,
         case when sykmelding_grad_prosent = 0 then 100 else sykmelding_grad_prosent end,
         lastet_dato
  FROM {{ ref('fk_dk_sensitiv__syk_sykmelding') }}
  WHERE sykmeldt_til_dato >= trunc(sysdate - 14)
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
