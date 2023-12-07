WITH agg_syk_p AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'AGG_SYK_SMP_SM_VAR_EGEN_MND') }}
)

SELECT * FROM agg_syk_p
