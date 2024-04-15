{{ config(
    materialized='table',
    post_hook = [
      "COMMENT ON COLUMN {{ this }}.fk_person1 IS 'Fremmednøkkel som refererer til person'",
      "COMMENT ON COLUMN {{ this }}.tilfelle_startdato IS 'Startdato for sykefraværstilfellet'",
      "COMMENT ON COLUMN {{ this }}.hendelse_tidspunkt IS 'Tidspunkt for hendelsen'",
      "COMMENT ON COLUMN {{ this }}.unntakarsak IS 'Årsak til unntaket'",
      "COMMENT ON COLUMN {{ this }}.kildesystem IS 'Kildesystem til hendelse'",
      "COMMENT ON COLUMN {{ this }}.lastet_dato IS 'Dato for last'"
    ]
)}}

/*
I denne modellen hentes siste unntaksårsak, som brukes i fak_dialogmote-tabellen.
*/
WITH hendelser as (
  SELECT * FROM {{ ref("mk_dialogmote__union") }}
),

final as (
/* Returnerer siste unntak/årsak innenfor sf-tilfellet */
select t1.fk_person1,
       t1.tilfelle_startdato,
       t1.hendelse_tidspunkt,
       t1.unntakarsak,
       t1.kildesystem,
       current_date as lastet_dato
from hendelser t1
inner join (/* Henter siste hendelsestidspunktet for unntaket innenfor sf-tilfellet */
            select fk_person1,
                   tilfelle_startdato,
                   max(hendelse_tidspunkt) as maks_hendelse_tidspunkt
            from hendelser
            where hendelse = 'UNNTAK'
            group by fk_person1,
                     tilfelle_startdato) t2
        on t2.fk_person1 = t1.fk_person1
       and t2.tilfelle_startdato = t1.tilfelle_startdato
       and t2.maks_hendelse_tidspunkt = t1.hendelse_tidspunkt
where t1.hendelse = 'UNNTAK')

select * from final
