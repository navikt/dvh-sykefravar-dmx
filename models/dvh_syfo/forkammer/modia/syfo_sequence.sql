with numbered_rows as (
  select fk_person1,FK_DIM_ALDER,
    row_number() OVER (partition  BY FK_person1 ORDER BY FK_PERSON1) as sequence_value
  from {{ ref('mk_modia__aktivitetskrav_flagg') }}
)
select
  numbered_rows.*,
  sequence_value as sequence_column
from numbered_rows
