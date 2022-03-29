with source_dim_varighet as (
  select * from {{ source('dmx_pox_oppfolging', 'DIM_VARIGHET') }}
),

final as (
  select * from source_dim_varighet
)

select * from final
