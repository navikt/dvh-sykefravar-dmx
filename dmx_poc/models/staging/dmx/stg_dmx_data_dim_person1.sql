
with source_dim_person1 as (
    select  * from {{ source('dmx_poc_person', 'DIM_PERSON1') }}
),

final as (
    select * from source_dim_person1
)

select * from final
