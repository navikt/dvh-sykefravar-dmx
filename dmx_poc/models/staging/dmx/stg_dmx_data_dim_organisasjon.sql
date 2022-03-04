{{ config(
    pre_hook=[
      "drop  view {{this}}"
    ]
) }}



with source_dim_organisasjon as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_ORG') }}
),

final as (
    select * from source_dim_organisasjon
)

select * from final
