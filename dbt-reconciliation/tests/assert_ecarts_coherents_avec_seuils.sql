-- Un statut ECART_TOLERE ne doit jamais franchir les deux seuils :
-- si c'était le cas, la règle de qualification serait incohérente avec elle-même.
-- Ce test protège la règle de gestion, pas la donnée.

select
    campagne_id,
    placement_id,
    date_diffusion,
    ecart_ca_pct,
    ecart_ca_eur,
    statut
from {{ ref('gold_reconciliation') }}
where statut = 'ECART_TOLERE'
  and abs(ecart_ca_pct) > {{ var('seuil_ca_pct') }}
  and abs(ecart_ca_eur) > {{ var('seuil_ca_abs_eur') }}
