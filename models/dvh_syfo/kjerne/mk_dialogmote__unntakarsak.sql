{{ config(
    materialized='table'
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
       t1.unntakarsak
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