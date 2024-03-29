SELECT
  rownum AS id,
  fnr,
  TO_NUMBER(
    gj_uforg, '9D999999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''
  ) AS gj_uforg,
  TO_DATE(s_start, 'YYYYMMDD') AS s_start,
  TO_DATE(s_stopp, 'YYYYMMDD') AS s_stopp,
  TO_DATE(p_start, 'YYYYMMDD') AS p_start,
  TO_DATE(p_slutt, 'YYYYMMDD') AS p_slutt
FROM {{ source('syfra', 'ssb_syfra_teller_kv') }}
