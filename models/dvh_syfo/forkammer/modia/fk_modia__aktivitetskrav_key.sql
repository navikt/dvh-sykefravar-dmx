WITH aktivitetskrav_key as (
  select *
  from {{source('modia','aktivitetskrav_key')}}
)

select * from aktivitetskrav_key