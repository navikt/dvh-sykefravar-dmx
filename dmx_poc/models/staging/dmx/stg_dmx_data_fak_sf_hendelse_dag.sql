{{ config(materialized='table') }}

with source_fak_sf_hendelse as (
    select  * from {{ source ('dmx_pox_oppfolging', 'FAK_SF_HENDELSE_DAG')}} 
),

final as (
    select * from source_fak_sf_hendelse where FK_DIM_SF_HENDELSESTYPE = 244 
    and FK_DIM_TID_IDENT_DATO > 20210101 and GYLDIG_FLAGG = 1
)

select * from final