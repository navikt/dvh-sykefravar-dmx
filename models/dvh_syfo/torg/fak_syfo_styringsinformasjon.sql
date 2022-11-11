
with final AS (
    SELECT * FROM {{ref('mk_dialogmote__join_fk_person1') }}
     where HENDELSE = 'INNKALT' and KILDESYSTEM = 'MODIA'
)

SELECt * FROM final

