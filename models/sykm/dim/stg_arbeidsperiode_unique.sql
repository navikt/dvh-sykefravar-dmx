{{ config(
    tags=["IA_PIA"]
) }}

with source_arbeidsperiode_s as (
    select  * from {{ source('dmx_pox_dialogmote', 'ARBEIDS_PERIODE_SMALL') }}
),

final as (
    select * from source_arbeidsperiode_s
)

select * from final
