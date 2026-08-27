-- Les chiffres remontés par l'adserver : ce qui a été réellement diffusé.
select
    campagne_id,
    placement_id,
    date_diffusion,
    campagne_libelle as libelle_adserver,
    impressions      as impressions_adserver,
    ca_eur           as ca_adserver_eur
from {{ source('reconciliation', 'silver_diffusion') }}
where source = 'adserver'
