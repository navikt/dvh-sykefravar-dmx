with source_dim_naering as (
  select * from {{ source('dmx_pox_oppfolging', 'DIM_NAERING') }}
),

final as (
  select * from source_dim_naering
)

select * from final
