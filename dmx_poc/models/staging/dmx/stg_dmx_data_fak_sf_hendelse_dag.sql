{{ config(materialized='table') }}

with source_fak_sf_hendelse as (
    select  * from {{ source ('dmx_pox_oppfolging', 'FAK_SF_HENDELSE_DAG')}} 
),

final as (
    select * from source_fak_sf_hendelse where FK_DIM_SF_HENDELSESTYPE = 244 
    and extract(Year from LASTET_DATO) > 2020
)

select * from final