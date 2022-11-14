
with final AS (
    SELECT * FROM {{ref('mk_dialogmote__join_fk_person1') }}
     where HENDELSE = 'AVLYST' and KILDESYSTEM = 'MODIA'
)

SELECt * FROM final

