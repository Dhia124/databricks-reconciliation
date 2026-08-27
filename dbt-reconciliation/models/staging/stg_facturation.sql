-- Les chiffres du système de facturation : ce qui a été facturé au client.
select
    campagne_id,
    placement_id,
    date_diffusion,
    campagne_libelle as libelle_facturation,
    impressions      as impressions_facturation,
    ca_eur           as ca_facturation_eur
from {{ source('reconciliation', 'silver_diffusion') }}
where source = 'facturation'
