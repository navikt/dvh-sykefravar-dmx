

with source_fak_sf_oppfolging as (
    select  * from {{ source ('dmx_pox_oppfolging', 'FAK_SF_OPPFOLGING_D2_MND')}} 
),

final as (
    select * from source_fak_sf_oppfolging
)

select * from final