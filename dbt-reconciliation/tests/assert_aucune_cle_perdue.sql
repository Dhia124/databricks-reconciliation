-- Aucune clé métier ne doit se perdre entre silver et gold.
-- Si ce test échoue, la jointure a un problème de clé — c'est le premier
-- endroit où regarder.
--
-- C'est la traduction en test dbt de l'assertion qui vivait en fin de notebook.
-- La différence est de nature : là-bas elle bloquait l'exécution ; ici elle est
-- exécutée par `dbt test`, tracée, et ne peut pas être oubliée.

with cles_silver as (
    select count(*) as n
    from (
        select distinct campagne_id, placement_id, date_diffusion
        from {{ source('reconciliation', 'silver_diffusion') }}
    )
),

lignes_gold as (
    select count(*) as n from {{ ref('gold_reconciliation') }}
)

-- un test dbt échoue s'il retourne au moins une ligne
select
    cles_silver.n as cles_en_silver,
    lignes_gold.n as lignes_en_gold
from cles_silver, lignes_gold
where cles_silver.n <> lignes_gold.n
