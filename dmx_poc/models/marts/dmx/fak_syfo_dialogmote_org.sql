WITH fak_syfo_dialogmote2 AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote')}}
),

dim_org AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_organisasjon')}}
),

final AS (
    SELECT fak_syfo_dialogmote2.*, dim_org.EK_ORG_NODE FROM fak_syfo_dialogmote2 
    LEFT JOIN dim_org on fak_syfo_dialogmote2.ENHET_NR = dim_org.mapping_node_kode
    WHERE dim_org.mapping_node_type = 'NORGENHET' 
    AND fak_syfo_dialogmote2.DIALOGMOTE_TIDSPUNKT 
    BETWEEN dim_org.funk_gyldig_fra_dato AND dim_org.funk_gyldig_til_dato
)

SELECt final.* FROM final