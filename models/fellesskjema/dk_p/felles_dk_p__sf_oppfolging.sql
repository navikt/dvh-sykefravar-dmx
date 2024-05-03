with source as (
      select * from {{ source('dk_p', 'sf_oppfolging') }}
)

select * from source
