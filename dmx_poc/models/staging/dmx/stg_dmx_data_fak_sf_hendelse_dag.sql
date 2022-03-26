{{ config(materialized='table') }}

with source_fak_sf_hendelse as (
    select  *  from {{ source ('dmx_pox_oppfolging', 'FAK_SF_HENDELSE_DAG')}} 
),

final as ( 
    select to_char(source_fak_sf_hendelse.pk_fak_sf_hendelse_dag) ||
     'a' ||to_char(source_fak_sf_hendelse.fk_person1) as key_dmx_arena,
     source_fak_sf_hendelse.*
    from source_fak_sf_hendelse where FK_DIM_SF_HENDELSESTYPE = 105 and
    --fk_dim_tid_dato_hendelse>= '20210101' and GYLDIG_FLAGG = 1
     FK_DIM_TID_IDENT_DATO > 20210101 and GYLDIG_FLAGG = 1
)

select * from final