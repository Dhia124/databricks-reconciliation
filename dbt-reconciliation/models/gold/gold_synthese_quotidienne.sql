{{ config(materialized='table') }}

-- La table que consomme le tableau de bord. Elle porte l'indicateur qui intéresse
-- le métier : combien d'euros ne sont pas expliqués aujourd'hui.

select
    date_diffusion,
    count(*)                                                       as lignes,
    sum(case when statut = 'CONFORME'           then 1 else 0 end) as conformes,
    sum(case when statut = 'ECART_TOLERE'       then 1 else 0 end) as ecarts_toleres,
    sum(case when statut = 'ECART_SIGNIFICATIF' then 1 else 0 end) as ecarts_significatifs,
    sum(case when statut = 'ABSENT_ADSERVER'    then 1 else 0 end) as absents_adserver,
    sum(case when statut = 'ABSENT_FACTURATION' then 1 else 0 end) as absents_facturation,
    round(sum(coalesce(ca_adserver_eur, 0)), 2)                    as ca_adserver_eur,
    round(sum(coalesce(ca_facturation_eur, 0)), 2)                 as ca_facturation_eur,
    round(sum(coalesce(ca_facturation_eur, 0))
        - sum(coalesce(ca_adserver_eur, 0)), 2)                    as ecart_total_eur,
    round(sum(
        case when statut in ('ECART_SIGNIFICATIF', 'ABSENT_ADSERVER', 'ABSENT_FACTURATION')
             then abs(coalesce(ca_facturation_eur, 0) - coalesce(ca_adserver_eur, 0))
             else 0 end
    ), 2)                                                          as ca_a_expliquer_eur
from {{ ref('gold_reconciliation') }}
group by date_diffusion
