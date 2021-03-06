

WITH fak_syfo_dialogmote_org AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote')}}
),

dim_org AS (
    SELECT * FROM {{ref('stg_dim_organisasjon')}}
),

final AS (
    SELECT fak_syfo_dialogmote_org.*, dim_org.pk_dim_organisasjon as fk_dim_organisasjon
    FROM fak_syfo_dialogmote_org
    LEFT JOIN dim_org
    ON fak_syfo_dialogmote_org.enhet_nr = dim_org.mapping_node_kode
    WHERE dim_org.mapping_node_type = 'NORGENHET'
   AND fak_syfo_dialogmote_org.nyeste_dialogmote BETWEEN dim_org.gyldig_fra_dato AND dim_org.gyldig_til_dato
)

SELECt final.* FROM final