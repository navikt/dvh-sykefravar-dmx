WITH sourc1  (
  SELECT * from {{ source('dmx_pox_oppfolging','AGG_SYK_SMP_SM_VAR_EGEN_MND') }}
)

select * from source1