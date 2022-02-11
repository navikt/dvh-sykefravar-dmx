WITH fak_syfo_dialogmote3 AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote_org')}}
),

final AS (
    SELECT fak_syfo_dialogmote3.*, to_number(to_char(fak_syfo_dialogmote3.tilfelle_startdato, 'YYYYMMDD')) as fk_dim_tid_tilfelle_startdato FROM fak_syfo_dialogmote3 
    
)

SELECt final.* FROM final