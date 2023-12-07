
with source_dim_geografi as (
    select  * from {{ source('dt_p', 'dim_geografi') }}
),

final as (
    select * from source_dim_geografi
)

select * from final
