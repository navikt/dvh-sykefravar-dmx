{{ config(
    pre_hook=[
      "drop  view {{this}}"
    ]
) }}

with source_fak_sf_hendelse as (
    select  * from {{ source ('dmx_pox_oppfolging', 'FAK_SF_HENDELSE_DAG')}} 
),

final as (
    select * from source_fak_sf_hendelse
)

select * from final