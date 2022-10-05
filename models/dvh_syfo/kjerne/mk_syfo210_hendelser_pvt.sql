WITH hendelser as (
  SELECT
    aktuelle_hendelser.*,
    decode(hendelse, 'FERDIGSTILT', dialogmote_tidspunkt, hendelse_tidspunkt) as hendelse_tidspunkt1
  FROM {{ ref('mk_syfo210_hendelser') }}  aktuelle_hendelser
)
,
final as (
  select * from (--TODO rett opp hvis periode ikke skal inn her
  --select fk_person1, tilfelle_startdato1, periode, hendelse,hendelse_tidspunkt1
  select fk_person1, tilfelle_startdato, concat(hendelse, row_number) as hendelse1,hendelse_tidspunkt1
  from   hendelser
)
pivot (
  max(hendelse_tidspunkt1)
  for hendelse1 in (
    'STOPPUNKT1' STOPPUNKT, 'FERDIGSTILT1' DIALOGMOTE_TIDSPUNKT1,'FERDIGSTILT2' DIALOGMOTE_TIDSPUNKT2, 'UNNTAK1' UNNTAK
  )
)
order by fk_person1, tilfelle_startdato
)
select * from final
