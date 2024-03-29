{{ config(
	post_hook="grant read on {{ this }} to DVH_OPPGAVE_APP"
) }}


with hendelser_org AS (
    SELECT * FROM {{ref('mk_dialogmote__union') }}
     where HENDELSE in ('INNKALT','AVLYST','UNNTAK','FERDIGSTILT','STOPPUNKT') and KILDESYSTEM = 'MODIA'
),
final  as  (
        select * from
            (
                select
                fk_person1,
                ENHET_NR,
                hendelse,
                tilfelle_startdato,
                DIALOGMOTE_TIDSPUNKT,
                hendelse_tidspunkt,
                unntakarsak
                from hendelser_org
                group by
                fk_person1,
                ENHET_NR,
                hendelse,
                tilfelle_startdato,
                DIALOGMOTE_TIDSPUNKT,
                hendelse_tidspunkt,
                unntakarsak
             )
            pivot (
               count(HENDELSE) as flagg
            for hendelse
            in ('STOPPUNKT' KANDIDAT, 'UNNTAK' UNNTAK, 'INNKALT' INNKALT , 'FERDIGSTILT' FERDIGSTILT, 'AVLYST' AVLYST)
                 )

     )
SELECt * FROM final

