

with source_fs_dialogmote as (
    select  * from {{ source('dmx_pox_dialogmote', 'FK_ISDIALOGMOTE_DM2') }}
),

final as (
    select to_char(source_fs_dialogmote.DIALOGMOTE_TIDSPUNKT) || 'm' ||
             to_char(source_fs_dialogmote.fk_person1) as key_dmx, 
            source_fs_dialogmote.* 
            from source_fs_dialogmote
)

select * from final
