
WITH motebehov as (
  SELECT * FROM {{ ref("mk_motebehov__join_fk_person1_drp202301") }} where har_motebehov=1
)


SELECT * FROM final
