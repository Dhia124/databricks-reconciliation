# dbt — couche gold de la réconciliation

Ce projet dbt reprend la couche **gold** du pipeline. Bronze et silver restent en
PySpark ; gold passe en SQL versionné, testé et documenté.

## Pourquoi seulement la couche gold

Le partage n'est pas arbitraire.

**Bronze et silver restent en PySpark** parce qu'ils font ce que dbt ne sait pas faire :
lire des fichiers bruts, gérer l'ingestion incrémentale, isoler les rejets dans des tables
de quarantaine avec leur motif. C'est de l'ingestion, pas de la transformation.

**Gold passe en dbt** parce que tout y est de la transformation SQL sur des tables déjà
propres : une jointure, des calculs d'écart, une règle de qualification, un agrégat.
Écrit en PySpark, ce code est un notebook que personne ne relit. Écrit en dbt, il devient
un graphe de dépendances explicite, avec des tests exécutés à chaque livraison et une
documentation générée depuis le code.

## Ce que la bascule a changé

| Avant (notebook) | Après (dbt) |
|---|---|
| Seuils dans des widgets Databricks | `vars` dans `dbt_project.yml`, versionnés dans Git |
| `assert` en fin de notebook | test dbt tracé, exécuté par `dbt test` |
| Dépendances implicites entre cellules | graphe `ref()` explicite, ordre calculé par dbt |
| Documentation en cellules markdown | `schema.yml`, exposé par `dbt docs` |
| Notebook lancé à la main | `dbt build` idempotent, appelable depuis Airflow |

## Structure

```
models/
  sources.yml              silver_diffusion et ses tests de source
  staging/
    stg_adserver.sql       les chiffres réellement diffusés
    stg_facturation.sql    les chiffres facturés au client
  gold/
    gold_reconciliation.sql        une ligne par clé métier, écart et statut
    gold_synthese_quotidienne.sql  agrégat quotidien pour le tableau de bord
    schema.yml                     tests et documentation
tests/
  assert_aucune_cle_perdue.sql              aucune clé ne disparaît entre silver et gold
  assert_ecarts_coherents_avec_seuils.sql   la règle de qualification reste cohérente
```

17 tests : unicité de la clé métier, valeurs autorisées du statut, bornes des agrégats,
non-nullité des clés, plus les deux tests singuliers ci-dessus.

## Mise en route

```bash
pip install dbt-core dbt-databricks
cp profiles.yml.example ~/.dbt/profiles.yml   # puis compléter

export DATABRICKS_HOST=adb-xxxx.azuredatabricks.net
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
export DATABRICKS_TOKEN=dapi...

dbt deps      # installe dbt_utils
dbt build     # exécute les modèles puis les tests
dbt docs generate && dbt docs serve
```

Le `http_path` se trouve sous **SQL Warehouses > votre entrepôt > Connection details**.
Sur l'édition gratuite, l'entrepôt serverless convient.

## Les seuils

```yaml
vars:
  seuil_ca_pct: 2.0
  seuil_ca_abs_eur: 50.0
```

Un écart doit franchir **les deux** pour être signalé : 2 % sur une petite campagne peut
représenter trois euros, et cinquante euros sur une grosse campagne peut n'être qu'un
arrondi de consolidation. Une alerte à laquelle personne ne croit plus ne sert à rien.

Pour les surcharger ponctuellement :

```bash
dbt build --vars '{seuil_ca_pct: 1.0, seuil_ca_abs_eur: 20.0}'
```
