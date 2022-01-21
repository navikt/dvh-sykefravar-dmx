

with source_fs_dialogmote as (
    select  * from {{ source('dmx_pox_dialogmote', 'FK_ISDIALOGMOTE_DM2') }}
),

final as (
    select * from source_fs_dialogmote
)

select * from final
