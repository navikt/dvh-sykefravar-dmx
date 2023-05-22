with numbered_rows as (
  select fk_person1, FK_DIM_TID_SF_START_DATO,
    row_number() OVER (partition by fk_person1, FK_DIM_TID_SF_START_DATO ORDER BY fk_person1, FK_DIM_TID_SF_START_DATO) as sequence_value
  from {{ ref('mk_modia__aktivitetskrav_flagg') }}
)
select
  numbered_rows.*,
  sequence_value as sequence_column
from numbered_rows
