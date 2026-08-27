# Brancher dbt sur le DAG existant

Le DAG `reconciliation_quotidienne` enchaîne cinq notebooks Databricks. La bascule
remplace la tâche `gold_reconciliation` par une tâche dbt, bronze et silver inchangés.

## Le graphe après bascule

```
generer_sources >> bronze >> silver >> dbt_build >> ecarts_au_dessus_du_seuil
ecarts_au_dessus_du_seuil >> [alerter_le_metier, journee_conforme] >> fin
```

`dbt_build` remplace **deux** tâches : `gold_reconciliation` et `controle_qualite`.
C'est le point important — `dbt build` exécute les modèles *puis* leurs tests, dans le
bon ordre, et échoue si un test échoue. Le contrôle qualité n'est plus une étape séparée
qu'on peut oublier de brancher : il est indissociable de la construction.

## La tâche

```python
from airflow.providers.standard.operators.bash import BashOperator

dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=(
        "cd /opt/airflow/dbt-reconciliation && "
        "dbt build --target prod --fail-fast"
    ),
    env={
        "DATABRICKS_HOST":      "{{ conn.databricks_default.host }}",
        "DATABRICKS_HTTP_PATH": "{{ var.value.databricks_http_path }}",
        "DATABRICKS_TOKEN":     "{{ conn.databricks_default.password }}",
        "DBT_PROFILES_DIR":     "/opt/airflow/dbt-reconciliation",
    },
    append_env=True,
    on_failure_callback=alerter_echec,
)
```

`--fail-fast` arrête au premier test rouge plutôt que de continuer à construire sur une
base fausse. `on_failure_callback` réutilise l'alerte technique déjà écrite dans
`dags/utils/alertes.py` : rien à changer de ce côté.

## Monter le projet dans le conteneur

Dans `docker-compose.yaml` :

```yaml
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dbt-reconciliation:/opt/airflow/dbt-reconciliation
    environment:
      _PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-databricks dbt-databricks"
```

Puis, une fois le conteneur démarré :

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt-reconciliation && dbt deps"
```

## Récupérer le montant pour le branchement

La tâche `ecarts_au_dessus_du_seuil` lisait la sortie du notebook de contrôle via XCom.
Après bascule, elle interroge directement la table de synthèse :

```python
def lire_ecarts(**context):
    hook = DatabricksSqlHook(databricks_conn_id="databricks_default")
    lignes = hook.get_records(
        "SELECT ca_a_expliquer_eur "
        "FROM workspace.reconciliation.gold_synthese_quotidienne "
        "ORDER BY date_diffusion DESC LIMIT 1"
    )
    montant = float(lignes[0][0]) if lignes else 0.0
    return "alerter_le_metier" if montant > SEUIL_ALERTE_EUR else "journee_conforme"
```

Plus robuste que de faire transiter la valeur par XCom : la source de vérité devient la
table, pas un message entre deux tâches.
