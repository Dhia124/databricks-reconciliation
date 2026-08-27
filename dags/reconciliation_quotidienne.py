"""Orchestration quotidienne du pipeline de réconciliation inter-sources.

Le pipeline Databricks (bronze → silver → gold) existait déjà et se lançait à la
main, notebook après notebook. Ce DAG le rend autonome : il l'exécute chaque
matin, réessaie ce qui échoue, prévient quand ça casse, et — c'est le point qui
compte — décide en fin de chaîne s'il faut alerter le métier.

Deux modes, pilotés par la variable d'environnement MODE_EXECUTION :

  simulation  le graphe s'exécute sans workspace Databricks. Sert à valider la
              structure du DAG, les dépendances et le branchement sans consommer
              de compute. C'est le mode par défaut.
  databricks  chaque tâche lance réellement le notebook correspondant via
              l'API Jobs, avec la connexion Airflow `databricks_default`.

Le graphe est identique dans les deux modes : seule l'implémentation des tâches
change. Une erreur de dépendance se voit donc en simulation, avant de coûter
la moindre minute de compute.
"""

from __future__ import annotations

import json
import os
import random
import time

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.alertes import alerter_echec, alerter_ecarts

MODE = os.getenv("MODE_EXECUTION", "simulation").lower()
PREFIXE = os.getenv("DATABRICKS_NOTEBOOK_PREFIX", "/Workspace/reconciliation")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID") or None
CONN_ID = "databricks_default"

CATALOG = "workspace"
SCHEMA = "reconciliation"
SEUIL_CA_PCT = "2.0"
SEUIL_CA_ABS_EUR = "50.0"

# Au-dela de ce montant non explique sur une journee, on previent le metier.
SEUIL_ALERTE_EUR = 500.0

PARAMS_COMMUNS = {"catalog": CATALOG, "schema": SCHEMA}
PARAMS_GOLD = {
    **PARAMS_COMMUNS,
    "seuil_ca_pct": SEUIL_CA_PCT,
    "seuil_ca_abs_eur": SEUIL_CA_ABS_EUR,
}


# --------------------------------------------------------------------------- #
# Acces a l'API Databricks
# --------------------------------------------------------------------------- #

def _connexion():
    """Adresse et jeton, lus depuis la connexion Airflow — jamais en dur."""
    from airflow.hooks.base import BaseHook

    c = BaseHook.get_connection(CONN_ID)
    host = (c.host or "").rstrip("/")
    if not host.startswith("http"):
        host = "https://" + host
    jeton = c.password or c.extra_dejson.get("token")
    return host, str(jeton)


def _sortie_notebook(run_id) -> str:
    """Valeur renvoyee par `dbutils.notebook.exit` pour un run donne.

    L'operateur pousse en XCom l'identifiant du run, pas la sortie du notebook.
    On va donc la chercher : un run multi-taches porte ses taches dans `tasks`,
    chacune avec son propre run_id, et c'est celui-la que `runs/get-output`
    attend.
    """
    import requests

    host, jeton = _connexion()
    entetes = {"Authorization": "Bearer " + jeton}

    r = requests.get(
        f"{host}/api/2.1/jobs/runs/get",
        params={"run_id": run_id}, headers=entetes, timeout=30,
    )
    r.raise_for_status()
    taches = r.json().get("tasks") or []
    cible = taches[0]["run_id"] if taches else run_id

    r = requests.get(
        f"{host}/api/2.1/jobs/runs/get-output",
        params={"run_id": cible}, headers=entetes, timeout=30,
    )
    r.raise_for_status()
    return r.json().get("notebook_output", {}).get("result")


# --------------------------------------------------------------------------- #
# Fabrique de taches
# --------------------------------------------------------------------------- #

def _simuler_notebook(notebook: str, **context):
    """Mode simulation : trace ce qui serait lance, sans rien lancer.

    Le controle qualite renvoie une valeur plausible pour que le branchement en
    aval s'execute reellement — c'est la logique de decision qu'on veut tester.
    """
    print(f"[simulation] notebook {notebook}")
    time.sleep(1)
    if notebook.endswith("04_controle_qualite"):
        montant = round(random.Random(context["ds"]).uniform(0, 1500), 2)
        sortie = {"jour": context["ds"], "ca_a_expliquer_eur": montant}
        print(f"[simulation] sortie du controle : {sortie}")
        return json.dumps(sortie)
    return None


def tache_notebook(task_id: str, notebook: str, parametres: dict):
    """Renvoie l'operateur adapte au mode courant, a task_id identique."""
    if MODE == "databricks":
        from airflow.providers.databricks.operators.databricks import (
            DatabricksSubmitRunOperator,
        )

        # Format multi-taches obligatoire : un workspace en serverless refuse la
        # forme simple avec "Only serverless compute is supported". Sans cle de
        # compute dans la tache, Databricks l'execute en serverless.
        tache = {
            "task_key": task_id,
            "notebook_task": {
                "notebook_path": f"{PREFIXE}/{notebook}",
                "base_parameters": parametres,
            },
        }
        if CLUSTER_ID:
            tache["existing_cluster_id"] = CLUSTER_ID

        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id=CONN_ID,
            json={"run_name": f"airflow__{task_id}", "tasks": [tache]},
            do_xcom_push=True,
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=_simuler_notebook,
        op_kwargs={"notebook": notebook},
    )


def _lire_sortie_controle(**context) -> dict:
    """Recupere la valeur renvoyee par le notebook 04, quel que soit le mode.

    Un echec silencieux ici ferait passer une journee d'ecarts pour une journee
    conforme — exactement l'inverse du but du pipeline. On echoue donc
    explicitement plutot que de supposer une valeur par defaut.
    """
    ti = context["task_instance"]

    if MODE == "databricks":
        run_id = ti.xcom_pull(task_ids="controle_qualite", key="run_id")
        if not run_id:
            raise ValueError("Aucun run_id remonte par la tache controle_qualite.")
        brut = _sortie_notebook(run_id)
    else:
        brut = ti.xcom_pull(task_ids="controle_qualite", key="return_value")

    if not brut:
        raise ValueError(
            "Le notebook 04_controle_qualite n'a rien renvoye. Verifier qu'il se "
            "termine bien par dbutils.notebook.exit(json.dumps(...))."
        )
    return brut if isinstance(brut, dict) else json.loads(brut)


def _decider_alerte(**context) -> str:
    """Branche metier : au-dela du seuil, on reveille quelqu'un."""
    sortie = _lire_sortie_controle(**context)
    montant = float(sortie["ca_a_expliquer_eur"])
    print(f"CA a expliquer le {sortie['jour']} : {montant} EUR (seuil {SEUIL_ALERTE_EUR})")
    return "alerter_le_metier" if montant > SEUIL_ALERTE_EUR else "journee_conforme"


def _alerter(**context) -> None:
    sortie = _lire_sortie_controle(**context)
    alerter_ecarts(
        montant_eur=float(sortie["ca_a_expliquer_eur"]),
        seuil_eur=SEUIL_ALERTE_EUR,
        jour=sortie["jour"],
    )


# --------------------------------------------------------------------------- #
# Le DAG
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="reconciliation_diffusion_quotidienne",
    description="Ingestion, controles qualite et reconciliation des ecarts de CA",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,          # pas de rattrapage retroactif au premier demarrage
    max_active_runs=1,      # deux executions concurrentes ecriraient les memes tables
    default_args={
        "owner": "dhia",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": alerter_echec,
    },
    tags=["databricks", "qualite", "reconciliation"],
) as dag:

    # En production, cette tache serait un capteur attendant le depot des
    # fichiers du jour. Ici elle genere les sources synthetiques du projet.
    generer_sources = tache_notebook(
        "generer_sources", "00_generate_sources", PARAMS_COMMUNS
    )

    bronze = tache_notebook("bronze", "01_bronze", PARAMS_COMMUNS)
    silver = tache_notebook("silver", "02_silver", PARAMS_COMMUNS)
    gold = tache_notebook("gold_reconciliation", "03_gold_reconciliation", PARAMS_GOLD)
    controle = tache_notebook("controle_qualite", "04_controle_qualite", PARAMS_COMMUNS)

    decision = BranchPythonOperator(
        task_id="ecarts_au_dessus_du_seuil",
        python_callable=_decider_alerte,
    )

    alerte = PythonOperator(task_id="alerter_le_metier", python_callable=_alerter)
    conforme = EmptyOperator(task_id="journee_conforme")

    # none_failed_min_one_success : la fin du DAG est un succes quelle que soit
    # la branche empruntee. Sans cette regle, la branche non prise laisserait le
    # run en "skipped" et masquerait les vraies pannes.
    fin = EmptyOperator(
        task_id="fin", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    generer_sources >> bronze >> silver >> gold >> controle >> decision
    decision >> [alerte, conforme] >> fin
