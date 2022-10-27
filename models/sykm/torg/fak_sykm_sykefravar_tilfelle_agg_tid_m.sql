
 select * from
  {{ metrics.calculate(
      [metric('fak_sykm_sykefravar_tilfelle_agg_tid_m.antall')],
      grain='week',
      dimensions=['fylke_navn']
  }}
