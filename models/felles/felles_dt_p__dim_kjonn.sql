
with source_dim_kj as (
    select  * from {{ source('dt_p', 'dim_kjonn') }}
),

final as (
    select * from source_dim_kj
)

select * from final
