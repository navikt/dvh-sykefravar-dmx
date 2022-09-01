{{ config(
    tags=["IA_PIA"]
) }}

with source_arbeidsperiode as (
    select  * from {{ source('dmx_pov_sykefravar_andre', 'ARBEIDSPERIODE') }}
),

final as (
    select * from source_arbeidsperiode
)

select * from final
