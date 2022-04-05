
WITH fak_syfo_hendelse_v AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),
    dim_org AS (
    SELECT * FROM {{ref('stg_dmx_data_dim_organisasjon')}}
),

     final AS (
         SELECT fak_syfo_hendelse_v.*,dim_org.pk_dim_organisasjon as fk_dim_organisasjon
         --  fjerner den foreløpig bruker
         -- den som kommer fra tabellen dim_org.pk_dim_organisasjon as fk_dim_organisasjon
         FROM fak_syfo_hendelse_v
                  LEFT JOIN dim_org
                            ON fak_syfo_hendelse_v.fk_dim_organisasjon
                                = dim_org.pk_dim_organisasjon
         WHERE dim_org.mapping_node_type like 'ARENAENHET%'  and dim_org.GYLDIG_FLAGG=1

     )
SELECt final.* FROM final
