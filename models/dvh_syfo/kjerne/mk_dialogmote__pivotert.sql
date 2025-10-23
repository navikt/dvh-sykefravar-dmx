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
),

unntakarsak_modia as (
  select fk_person1,
         tilfelle_startdato,
         hendelse_tidspunkt1,
         unntakarsak as unntakarsak_modia
  from hendelser
  where hendelse='UNNTAK'
    and kildesystem = 'MODIA'
)

-- Får kun virksomhetsnr fra dialogmøter i Modia, så i union-tabellen får virksomhetsnr null-verdier idet flere tabeller sammenstilles.
-- Joiner denne i neste steg for å hindre feil i pivoteringen da vi får flere rader per fk_person1 + tilfelle_startdato (null fra Arena + kandidater og not-null fra dialogmøter i Modia).
,not_null_virksomhetsnr as (
    select fk_person1, tilfelle_startdato, max(virksomhetsnr) as virksomhetsnr
    from hendelser
    group by fk_person1, tilfelle_startdato
)

/* Finner dialogmøter som er avholdt av Regional Oppfølgingsenhet (ROE) Vest-Viken.
Nav-identer som jobber for ROE Vest-Viken ligger i tabellen dim_syfo_reg_oppf_enhet_ident.
Dersom en 'FERDIGSTILT'-hendelse er registrert med en av disse identene i tidsrommet identen er gyldig, blir region_oppf_enhet_vviken_flagg satt til 1.
NB! Må bruke min(dialogmote_tidspunkt) for ikkje å få med for mange rader. Skal berre hente eitt tidspunkt per tilfelle, */
, dialogmote_roe as (
  select h.fk_person1,
         h.tilfelle_startdato,
         min(h.hendelse_tidspunkt1) as hendelse_tidspunkt,
         1 as region_oppf_enhet_vviken_flagg
  from hendelser h
  join dim_syfo_reg_oppf_enhet_ident r on r.nav_ident = h.nav_ident
  where h.hendelse in ('FERDIGSTILT', 'UNNTAK')
  and trunc(h.hendelse_tidspunkt1) between r.gyldig_fra_dato and r.gyldig_til_dato
  group by h.fk_person1,
           h.tilfelle_startdato
)

,pivotert AS (
  SELECT * FROM (
    SELECT
      a.fk_person1
      ,a.tilfelle_startdato AS tilfelle_startdato
      ,CONCAT(a.hendelse, a.ROW_NUMBER) AS hendelse1
      ,a.hendelse_tidspunkt1
      ,b.virksomhetsnr
      ,nvl(c.region_oppf_enhet_vviken_flagg,0) as region_oppf_enhet_vviken_flagg
    FROM hendelser a
    left join not_null_virksomhetsnr b on
      a.fk_person1=b.fk_person1 and a.tilfelle_startdato=b.tilfelle_startdato
    left join dialogmote_roe c on
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
    SELECT distinct
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
      um.unntakarsak_modia,
      pivotert.region_oppf_enhet_vviken_flagg
    FROM pivotert
    left join unntakarsak_modia um on um.fk_person1 = pivotert.fk_person1
                                  and um.tilfelle_startdato = pivotert.tilfelle_startdato
                                  and um.hendelse_tidspunkt1 = pivotert.unntak
 )

select * from final