
WITH fak_syfo_hendelse_v AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),
    dim_org AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_organisasjon')}}
),

     final AS (
         SELECT fak_syfo_hendelse_v.*, dim_org.mapping_node_kode
         FROM fak_syfo_hendelse_v
                  LEFT JOIN dim_org
                            ON fak_syfo_hendelse_v.fk_dim_organisasjon
                                = dim_org.EK_ORG_NODE
         WHERE dim_org.mapping_node_type = 'ARENAENHET'
         --AND fak_syfo_hendelse_v.nyeste_dialogmote
         -- BETWEEN dim_org.funk_gyldig_fra_dato AND dim_org.funk_gyldig_til_dato
     )
SELECt final.* FROM final
