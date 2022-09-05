with source_dim_tid as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_TID') }}
),

final as (
    select * from source_dim_tid
)

select * from final
