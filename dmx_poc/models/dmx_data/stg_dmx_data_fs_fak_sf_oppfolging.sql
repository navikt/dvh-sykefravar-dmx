
with source_fak_sf_oppfolging as (
    select  * from {{ source('dmx_pox_dialogmote', 'dt_p.fak_sf_oppfolging_d2_mnd') }}
),

final as (
    select * from source_fak_sf_oppfolging
)

select * from final