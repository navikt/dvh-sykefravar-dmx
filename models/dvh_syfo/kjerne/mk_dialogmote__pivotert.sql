WITH hendelser as (
  SELECT
    aktuelle_hendelser.*
    ,DECODE(
      hendelse, 'FERDIGSTILT'
        ,dialogmote_tidspunkt
        ,hendelse_tidspunkt
    ) AS hendelse_tidspunkt1
  FROM {{ ref("mk_dialogmote__tidligste_tilfelle_startdato") }} aktuelle_hendelser
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
      ,CONCAT(a.hendelse, a.ROW_NUMBER) AS hendelse1
      ,a.hendelse_tidspunkt1
      ,b.virksomhetsnr
    FROM hendelser a
    left join not_null_virksomhetsnr b on
      a.fk_person1=b.fk_person1 and a.tilfelle_startdato=b.tilfelle_startdato
  )
  PIVOT(
    MAX(hendelse_tidspunkt1) FOR hendelse1 IN (
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
