
with source_dim_geografi as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_GEOGRAFI') }}
),

final as (
    select * from source_dim_geografi
)

select * from final
