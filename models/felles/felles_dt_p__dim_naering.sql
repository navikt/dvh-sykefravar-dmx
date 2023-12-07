
with source_dim_n as (
    select  * from {{ source('dt_p', 'dim_naering') }}
),

final as (
    select * from source_dim_n
)

select * from final
