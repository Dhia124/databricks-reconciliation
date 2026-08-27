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
la moindre minute de cluster.
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

CATALOG = "workspace"
SCHEMA = "reconciliation"
SEUIL_CA_PCT = "2.0"
SEUIL_CA_ABS_EUR = "50.0"

# Au-delà de ce montant non expliqué sur une journée, on prévient le métier.
SEUIL_ALERTE_EUR = 500.0

PARAMS_COMMUNS = {"catalog": CATALOG, "schema": SCHEMA}
PARAMS_GOLD = {
    **PARAMS_COMMUNS,
    "seuil_ca_pct": SEUIL_CA_PCT,
    "seuil_ca_abs_eur": SEUIL_CA_ABS_EUR,
}


# --------------------------------------------------------------------------- #
# Fabrique de tâches
# --------------------------------------------------------------------------- #

def _simuler_notebook(notebook: str, **context):
    """Mode simulation : trace ce qui serait lancé, sans rien lancer.

    Le contrôle qualité renvoie une valeur plausible pour que le branchement en
    aval s'exécute réellement — c'est la logique de décision qu'on veut tester.
    """
    print(f"[simulation] notebook {notebook} avec {PARAMS_GOLD}")
    time.sleep(1)
    if notebook.endswith("04_controle_qualite"):
        montant = round(random.Random(context["ds"]).uniform(0, 1500), 2)
        sortie = {"jour": context["ds"], "ca_a_expliquer_eur": montant}
        print(f"[simulation] sortie du contrôle : {sortie}")
        return json.dumps(sortie)
    return None


def tache_notebook(task_id: str, notebook: str, parametres: dict):
    """Renvoie l'opérateur adapté au mode courant, à task_id identique."""
    if MODE == "databricks":
        from airflow.providers.databricks.operators.databricks import (
            DatabricksSubmitRunOperator,
        )

        specification = {
            "notebook_task": {
                "notebook_path": f"{PREFIXE}/{notebook}",
                "base_parameters": parametres,
            }
        }
        if CLUSTER_ID:
            specification["existing_cluster_id"] = CLUSTER_ID
        else:
            # Cluster éphémère : démarre au run, s'éteint à la fin. Plus lent au
            # démarrage, mais rien ne tourne entre deux exécutions quotidiennes.
            specification["new_cluster"] = {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 1,
            }

        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id="databricks_default",
            json=specification,
            do_xcom_push=True,
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=_simuler_notebook,
        op_kwargs={"notebook": notebook},
    )


def _lire_sortie_controle(**context) -> dict:
    """Récupère la valeur renvoyée par le notebook 04.

    Selon le mode et la version du provider, la sortie arrive sous des clés
    différentes. On les essaie dans l'ordre plutôt que d'en supposer une : un
    échec silencieux ici ferait passer une journée d'écarts pour une journée
    conforme, ce qui est exactement l'inverse du but du pipeline.
    """
    ti = context["task_instance"]
    for cle in ("notebook_output", "return_value"):
        brut = ti.xcom_pull(task_ids="controle_qualite", key=cle)
        if brut:
            if isinstance(brut, dict):
                return brut
            try:
                return json.loads(brut)
            except (TypeError, ValueError):
                continue
    raise ValueError(
        "Le notebook 04_controle_qualite n'a rien renvoyé. Vérifier qu'il se "
        "termine bien par dbutils.notebook.exit(json.dumps(...))."
    )


def _decider_alerte(**context) -> str:
    """Branche métier : au-delà du seuil, on réveille quelqu'un."""
    sortie = _lire_sortie_controle(**context)
    montant = float(sortie["ca_a_expliquer_eur"])
    print(f"CA à expliquer le {sortie['jour']} : {montant} € (seuil {SEUIL_ALERTE_EUR} €)")
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
    description="Ingestion, contrôles qualité et réconciliation des écarts de CA",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,          # pas de rattrapage rétroactif au premier démarrage
    max_active_runs=1,      # deux exécutions concurrentes écriraient les mêmes tables
    default_args={
        "owner": "dhia",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": alerter_echec,
    },
    tags=["databricks", "qualite", "reconciliation"],
) as dag:

    # En production, cette tâche serait un capteur attendant le dépôt des
    # fichiers du jour. Ici elle génère les sources synthétiques du projet.
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

    # none_failed_min_one_success : la fin du DAG est un succès quelle que soit
    # la branche empruntée. Sans cette règle, la branche non prise laisserait le
    # run en « skipped » et masquerait les vraies pannes.
    fin = EmptyOperator(
        task_id="fin", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    generer_sources >> bronze >> silver >> gold >> controle >> decision
    decision >> [alerte, conforme] >> fin
