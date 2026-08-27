{{ config(materialized='table') }}

-- Le cœur du projet : les deux sources côte à côte sur la clé métier commune.
--
-- Le FULL OUTER JOIN n'est pas un détail de style. Un INNER JOIN ne verrait que
-- les lignes présentes des deux côtés — il masquerait exactement les anomalies
-- les plus graves : une diffusion facturée mais jamais remontée par l'adserver,
-- ou l'inverse. Les lignes manquantes d'un côté sont le premier motif d'écart,
-- pas un cas limite.

with adserver as (
    select * from {{ ref('stg_adserver') }}
),

facturation as (
    select * from {{ ref('stg_facturation') }}
),

joint as (
    select
        coalesce(a.campagne_id, f.campagne_id)              as campagne_id,
        coalesce(a.placement_id, f.placement_id)            as placement_id,
        coalesce(a.date_diffusion, f.date_diffusion)        as date_diffusion,
        -- le libellé vient de l'un ou l'autre côté selon la ligne manquante
        coalesce(a.libelle_adserver, f.libelle_facturation) as campagne_libelle,
        a.impressions_adserver,
        f.impressions_facturation,
        a.ca_adserver_eur,
        f.ca_facturation_eur
    from adserver a
    full outer join facturation f
        on  a.campagne_id    = f.campagne_id
        and a.placement_id   = f.placement_id
        and a.date_diffusion = f.date_diffusion
),

calculs as (
    select
        *,
        round(ca_facturation_eur - ca_adserver_eur, 2) as ecart_ca_eur,
        -- le dénominateur est protégé : une base à zéro ferait exploser le pourcentage
        round(
            (ca_facturation_eur - ca_adserver_eur)
            / greatest(abs(ca_adserver_eur), 0.01) * 100
        , 2) as ecart_ca_pct,
        impressions_facturation - impressions_adserver as ecart_impressions
    from joint
)

select
    campagne_id,
    campagne_libelle,
    placement_id,
    date_diffusion,
    impressions_adserver,
    impressions_facturation,
    ecart_impressions,
    ca_adserver_eur,
    ca_facturation_eur,
    ecart_ca_eur,
    ecart_ca_pct,
    -- Un écart doit franchir les DEUX seuils pour être signalé : en dessous,
    -- il relève de l'arrondi et du délai de consolidation.
    case
        when ca_adserver_eur    is null then 'ABSENT_ADSERVER'
        when ca_facturation_eur is null then 'ABSENT_FACTURATION'
        when abs(ecart_ca_pct) > {{ var('seuil_ca_pct') }}
         and abs(ecart_ca_eur) > {{ var('seuil_ca_abs_eur') }} then 'ECART_SIGNIFICATIF'
        when abs(ecart_ca_eur) > 0.01 then 'ECART_TOLERE'
        else 'CONFORME'
    end as statut,
    current_timestamp() as _date_calcul
from calculs
