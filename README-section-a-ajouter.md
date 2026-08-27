
---

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
