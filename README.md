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

## Réalisations

- Construction d'un pipeline médaillon complet, de l'ingestion brute jusqu'à une
  synthèse quotidienne directement exploitable par le métier.
- Ingestion incrémentale et idempotente : les checkpoints Auto Loader empêchent la
  réingestion des fichiers déjà traités.
- Mise en place de cinq règles de qualité déclaratives, avec quarantaine des lignes
  rejetées et conservation de leur motif.
- Déduplication déterministe par clé métier en conservant la dernière extraction connue.
- Réconciliation exhaustive par `FULL OUTER JOIN`, avec cinq statuts et un double seuil
  de tolérance paramétrable.
- Contrôle automatique de bout en bout garantissant qu'aucune clé métier ne disparaît
  entre les couches silver et gold.

## Technologies utilisées

| Technologie | Utilisation dans le projet |
|---|---|
| **Databricks** | Exécution des notebooks et environnement de développement |
| **Python** | Génération des données synthétiques et paramétrage du pipeline |
| **PySpark** | Typage, nettoyage, déduplication, contrôles qualité et réconciliation |
| **Spark Structured Streaming / Auto Loader** | Ingestion incrémentale des fichiers JSON avec checkpoints |
| **Delta Lake** | Stockage transactionnel des tables bronze, silver et gold |
| **Spark SQL** | Contrôles, agrégations et synthèse quotidienne |
| **Unity Catalog** | Gouvernance du catalogue, des schémas, volumes et tables |
| **Databricks Asset Bundles** | Configuration reproductible du projet via `databricks.yml` |

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


--

## Orchestration

Le pipeline ci-dessus se lançait à la main, notebook après notebook. Un DAG
Apache Airflow le rend autonome : il l'exécute chaque matin à 6 h, réessaie ce
qui échoue, prévient quand ça casse, et décide en fin de chaîne s'il faut
alerter le métier.

```
generer_sources → bronze → silver → gold_reconciliation → controle_qualite
                                                               ↓
                                                  ecarts_au_dessus_du_seuil
                                                        ↙            ↘
                                          alerter_le_metier    journee_conforme
                                                        ↘            ↙
                                                             fin
```

Le branchement est le cœur du DAG. Sans lui, l'orchestration ne ferait que
déplacer le lancement manuel de l'humain vers un ordonnanceur : c'est la décision
automatique d'alerter, ou non, qui rend la chaîne réellement autonome.

### Démarrer

```bash
cp .env.example .env
docker compose up
```

L'interface est sur http://localhost:8080 ; le mot de passe généré s'affiche dans
les logs au premier démarrage. Le mode par défaut est `simulation` : le graphe
s'exécute réellement, mais les tâches ne lancent aucun notebook. Cela permet de
valider la structure et le branchement sans consommer de compute Databricks.

Pour brancher un vrai workspace : importer les notebooks, créer un token d'accès
personnel, déclarer la connexion `databricks_default` dans Airflow, puis passer
`MODE_EXECUTION=databricks` dans `.env`.

### Décisions de conception

**`catchup=False`.** Sur un pipeline qui écrase ses tables en `overwrite`, le
rattrapage rétroactif lancerait des dizaines d'exécutions sans rien apporter.

**`max_active_runs=1`.** Deux exécutions concurrentes écriraient les mêmes tables
Delta au même moment. La contrainte est posée dans le DAG plutôt que dans un
verrou côté notebooks.

**Deux tentatives, cinq minutes d'écart.** La plupart des échecs sur ce type de
chaîne sont transitoires — un cluster lent à démarrer, un appel API qui expire.
Au-delà, le problème est réel et le callback prévient.

**Le contrôle qualité est un notebook séparé.** Il ne transforme rien : il
vérifie qu'aucune clé métier ne s'est perdue entre silver et gold, calcule le
montant restant à expliquer, et le renvoie à Airflow via `dbutils.notebook.exit`.
Le sortir de la chaîne de transformation permet de le rejouer seul et rend
visible, dans le graphe, l'endroit où la décision se prend.

**Lecture défensive de la sortie du notebook.** Selon la version du provider, la
valeur revient sous `notebook_output` ou `return_value`. Le DAG essaie les deux
et échoue explicitement si aucune ne répond : un échec silencieux ferait passer
une journée d'écarts pour une journée conforme.

**Deux alertes de nature différente.** L'alerte technique s'adresse à qui
maintient le pipeline ; l'alerte d'écart s'adresse au métier, et signale un
résultat à traiter alors que le pipeline a parfaitement fonctionné. Les mélanger
est le meilleur moyen de faire ignorer les deux.
## Trophées GitHub

[![trophy](https://trophy.ryglcloud.net/?username=Dhia124)](https://github.com/ryo-ma/github-profile-trophy)
