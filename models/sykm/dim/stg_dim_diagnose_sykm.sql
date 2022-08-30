
{{ config(
    post_hook="grant read on {{this}} to dvh_syfra"
) }}

with source_dim_diagnose as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_DIAGNOSE') }}
),

final as (
    select * from source_dim_diagnose
)

select * from final
