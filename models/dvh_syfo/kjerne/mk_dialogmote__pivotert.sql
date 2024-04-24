WITH hendelser as (
  SELECT aktuelle_hendelser.fk_person1,
         aktuelle_hendelser.tilfelle_startdato,
         aktuelle_hendelser.hendelse,
         aktuelle_hendelser.hendelse_tidspunkt,
         aktuelle_hendelser.dialogmote_tidspunkt,
         aktuelle_hendelser.unntakarsak,
         aktuelle_hendelser.enhet_nr,
         aktuelle_hendelser.arbeidstaker_flagg,
         aktuelle_hendelser.arbeidsgiver_flagg,
         aktuelle_hendelser.sykmelder_flagg,
         aktuelle_hendelser.kilde_uuid,
         aktuelle_hendelser.kildesystem,
         aktuelle_hendelser.virksomhetsnr,
         aktuelle_hendelser.row_number,
         aktuelle_hendelser.nav_ident,
         DECODE(hendelse,'FERDIGSTILT', dialogmote_tidspunkt, hendelse_tidspunkt) AS hendelse_tidspunkt1
  FROM {{ ref("mk_dialogmote__tidligste_tilfelle_startdato") }} aktuelle_hendelser
)

,unntakarsak as (
  /* henter første rad/unntaksårsak for sykefraværstilfellet for hendelser = 'UNNTAK' */
  select fk_person1,
         hendelse_tidspunkt1,
         tilfelle_startdato,
         unntakarsak
  from hendelser
  where hendelse = 'UNNTAK'
    and row_number = 1
)

-- Får kun virksomhetsnr fra dialogmøter i Modia, så i union-tabellen får virksomhetsnr null-verdier idet flere tabeller sammenstilles.
-- Joiner denne i neste steg for å hindre feil i pivoteringen da vi får flere rader per fk_person1 + tilfelle_startdato (null fra Arena + kandidater og not-null fra dialogmøter i Modia).
,not_null_virksomhetsnr as (
    select fk_person1, tilfelle_startdato, max(virksomhetsnr) as virksomhetsnr
    from hendelser
    group by fk_person1, tilfelle_startdato
)

/* Flagg for regional oppfølgingsenhet Vest-Viken for gitte nav-identer.
Dersom en hendelse er registrert med en identene under, skal flagget settes til 1.
Velger max-verdi for at alle rader tilknyttet et tilfelle skal indikere at minst ett av hendelsene har kommet inn med denne identen.
Dersom ikke max-verdi settes, vil pivoteringen resultere i flere rader per tilfelle der det er ulike identer.  */
,not_null_region_oppf_enhet_vviken_flagg as (
    select fk_person1, tilfelle_startdato, max(case when nav_ident in ('B160279', 'SNA0624', 'ELE0602', 'MOH0219') then 1 end) as region_oppf_enhet_vviken_flagg
    from hendelser
    group by fk_person1, tilfelle_startdato
)

,pivotert AS (
  SELECT * FROM (
    SELECT
      a.fk_person1
      ,a.tilfelle_startdato AS tilfelle_startdato
      ,CONCAT(a.hendelse, a.ROW_NUMBER) AS hendelse1
      ,a.hendelse_tidspunkt1
      ,b.virksomhetsnr
      ,c.region_oppf_enhet_vviken_flagg
    FROM hendelser a
    left join not_null_virksomhetsnr b on
      a.fk_person1=b.fk_person1 and a.tilfelle_startdato=b.tilfelle_startdato
    left join not_null_region_oppf_enhet_vviken_flagg c on
        a.fk_person1=c.fk_person1 and a.tilfelle_startdato=c.tilfelle_startdato
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
),

final as (
    SELECT
      pivotert.fk_person1,
      pivotert.tilfelle_startdato,
      pivotert.virksomhetsnr,
      pivotert.stoppunkt,
      pivotert.dialogmote_tidspunkt1,
      pivotert.dialogmote_tidspunkt2,
      pivotert.dialogmote_tidspunkt3,
      pivotert.dialogmote_tidspunkt4,
      pivotert.dialogmote_tidspunkt5,
      pivotert.dialogmote_tidspunkt6,
      pivotert.unntak,
      ua.unntakarsak
    FROM pivotert
    left join unntakarsak ua on ua.fk_person1 = pivotert.fk_person1
                            and ua.hendelse_tidspunkt1 = pivotert.unntak
                            and ua.tilfelle_startdato  = pivotert.tilfelle_startdato
 )

select * from final