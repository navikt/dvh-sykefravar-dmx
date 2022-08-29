{{ config(
    post_hook="grant read on {{this}} to dvh_syfra"
) }}

with source_dim_tid as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_TID') }}
),

final as (
    select * from source_dim_tid
)

select * from final
