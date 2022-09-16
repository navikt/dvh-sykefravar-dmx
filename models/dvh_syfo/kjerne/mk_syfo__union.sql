WITH union_all AS (
  {{ dbt_utils.union_relations(
    relations=[ref('fk_modia__kandidat'), ref('fk_modia__dialogmote__dummy__fix202210')],
    source_column_name=None
  ) }}
)

SELECT * FROM union_all
