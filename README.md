# Réconciliation inter-sources sur Databricks

Pipeline de détection d'écarts entre deux systèmes censés porter les mêmes chiffres de
diffusion publicitaire, construit sur une architecture médaillon avec Auto Loader,
Delta Lake et contrôles de qualité avec mise en quarantaine.

## Le problème traité

Deux systèmes remontent les mêmes campagnes avec des chiffres différents :

- **`adserver`** — remontée à J-1, partielle, non consolidée
- **`facturation`** — chiffres révisés jusqu'à J+8, avec ses propres anomalies de saisie

Chaque euro d'écart non expliqué entre les deux est un euro potentiellement mal facturé.
Le rapprochement se fait souvent à la main, sous Excel, par la personne qui gère le
produit. Ce pipeline l'automatise et rend l'écart mesurable jour après jour.

## Architecture

| Couche | Table | Rôle |
|---|---|---|
| Bronze | `bronze_adserver`, `bronze_facturation` | Donnée brute, schéma explicite, traçabilité du fichier d'origine |
| Silver | `silver_diffusion` | Typée, dédoublonnée, conforme aux règles de qualité |
| Silver | `silver_quarantaine` | Lignes rejetées **avec leur motif**, pas supprimées |
| Gold | `gold_reconciliation` | Une ligne par clé métier, écarts calculés et qualifiés |
| Gold | `gold_synthese_quotidienne` | Agrégat par jour, dont le CA restant à expliquer |

Clé métier de bout en bout : `campagne_id` + `placement_id` + `date_diffusion`.

## Décisions de conception

**`FULL OUTER JOIN`, pas `INNER`.** Un `INNER JOIN` masquerait les anomalies les plus
graves — une diffusion facturée mais jamais remontée, ou l'inverse. Les lignes absentes
d'un côté sont le premier motif d'écart, pas un cas limite.

**Quarantaine plutôt que suppression.** Une ligne invalide jetée fait disparaître le
problème sans le résoudre. Conservée avec son motif, elle devient exploitable par
l'équipe métier, qui peut corriger à la source.

**Double seuil de tolérance.** Un écart doit dépasser à la fois un pourcentage et un
montant absolu pour être signalé. Le pourcentage seul ferait remonter des centimes sur
les petits placements ; le montant seul laisserait passer des dérives relatives sur les
gros. Les deux ensemble donnent une liste que le métier peut réellement traiter.

**Schéma explicite en bronze.** Plutôt que de laisser Auto Loader inférer, on déclare le
schéma et on surveille `_rescued_data`. Une dérive de format devient visible au lieu de
se propager silencieusement.

**Déduplication par fenêtre.** La source facturation produit des doublons ; on conserve
la dernière extraction par clé métier via `row_number()` plutôt qu'un `dropDuplicates`
aveugle, qui garderait une ligne arbitraire.

## Exécution

Prérequis : un workspace Databricks (la Free Edition suffit) avec Unity Catalog.

| Notebook | Rôle |
|---|---|
| `00_generate_sources.py` | Génère 30 jours de données dans un volume, un fichier par jour |
| `01_bronze.py` | Auto Loader vers les tables bronze — idempotent grâce au checkpoint |
| `02_silver.py` | Typage, déduplication, 5 règles de qualité, quarantaine |
| `03_gold_reconciliation.py` | Jointure, écarts, statuts, synthèse quotidienne |

Les notebooks sont paramétrés par widgets (`catalog`, `schema`, `seuil_ca_pct`,
`seuil_ca_abs_eur`). Exécutez-les dans l'ordre.

Pour vérifier que l'ingestion est bien incrémentale : après un premier passage complet,
relancez la dernière cellule du notebook `00`, puis le `01`. Seul le nouveau fichier est
traité — le checkpoint fait son travail.

## Données

Entièrement synthétiques, générées avec une graine fixe : cinq campagnes, une
cinquantaine de placements, trente jours. La divergence entre les deux sources est
volontaire et paramétrée — sous-comptage à J-1, lignes manquantes, doublons, erreurs de
saisie d'un facteur 10, valeurs négatives. Aucune donnée réelle n'est utilisée.

## Suite

- Orchestration en job Databricks Workflows, avec dépendances et alerte sur échec
- Tableau de bord Databricks SQL ou Power BI branché sur `gold_synthese_quotidienne`
- Réécriture de la couche silver en Delta Live Tables avec `expect_or_drop`
- Historisation des écarts, pour suivre la dérive dans le temps plutôt qu'à un instant t
[![trophy](https://github-profile-trophy.vercel.app/?username=Dhia124)](https://github.com/ryo-ma/github-profile-trophy)
