/* Bruker tidspunkt for referat ferdigstilt (hendelse='FERDIGSTILT') til å telle dialogmøter,
og ikke angitt tidspunkt for når dialogmøte er kalt i inn til (dialogmote_tidspunkt)  */

WITH hendelser as (
  SELECT
    *
  FROM {{ ref("mk_dialogmote__tidligste_tilfelle_startdato") }}
)

-- Får kun virksomhetsnr fra dialogmøter i Modia, så i union-tabellen får virksomhetsnr null-verdier idet flere tabeller sammenstilles.
-- Joiner denne i neste steg for å hindre feil i pivoteringen da vi får flere rader per fk_person1 + tilfelle_startdato (null fra Arena + kandidater og not-null fra dialogmøter i Modia).
, not_null_virksomhetsnr as (
    select fk_person1, tilfelle_startdato, max(virksomhetsnr) as virksomhetsnr
    from hendelser
    group by fk_person1, tilfelle_startdato
)

,final AS (
  SELECT * FROM (
    SELECT
      a.fk_person1
      ,a.tilfelle_startdato AS tilfelle_startdato
      ,CONCAT(a.hendelse, a.ROW_NUMBER) AS hendelse
      ,a.hendelse_tidspunkt
      ,b.virksomhetsnr
    FROM hendelser a
    left join not_null_virksomhetsnr b on
      a.fk_person1=b.fk_person1 and a.tilfelle_startdato=b.tilfelle_startdato
  )
  PIVOT(
    MAX(hendelse_tidspunkt) FOR hendelse IN (
      'STOPPUNKT1' stoppunkt
      ,'FERDIGSTILT1' dialogmote_tidspunkt1
      ,'FERDIGSTILT2' dialogmote_tidspunkt2
      ,'FERDIGSTILT3' dialogmote_tidspunkt3
      ,'FERDIGSTILT4' dialogmote_tidspunkt4
      ,'FERDIGSTILT5' dialogmote_tidspunkt5
      ,'FERDIGSTILT6' dialogmote_tidspunkt6
      ,'UNNTAK1' unntak
    )
  )
  ORDER BY
    fk_person1
    ,tilfelle_startdato
)

SELECT * FROM final
