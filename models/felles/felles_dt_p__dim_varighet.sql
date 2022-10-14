
with source_dim_varighet as (
    select  * from {{ source('dt_p', 'dim_varighet') }}
),

final as (
    select * from source_dim_varighet
)

select * from final
