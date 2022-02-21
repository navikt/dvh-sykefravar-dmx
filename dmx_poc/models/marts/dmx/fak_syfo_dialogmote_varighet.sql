WITH fak_syfo_dialogmote_varighet AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote_tid')}}
),

dim_varighet AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_varighet')}}
),

final AS (
    SELECT fak_syfo_dialogmote_varighet.*,
           dim_varighet.pk_dim_varighet as fk_dim_varighet
    FROM fak_syfo_dialogmote_varighet
    LEFT JOIN dim_varighet
    ON dim_varighet.varighet_dager = trunc(fak_syfo_dialogmote_varighet.nyeste_dialogmote) - trunc(fak_syfo_dialogmote_varighet.nyeste_tilfelle_startdato)
)

SELECt final.* FROM final