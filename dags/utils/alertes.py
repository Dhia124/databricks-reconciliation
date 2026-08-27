"""Callbacks d'alerte partagés par les DAG.

Volontairement découplé du DAG : le jour où l'alerte part vers Slack, Teams ou
une boîte mail, c'est ce fichier qui change, pas la définition du graphe.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def alerter_echec(context: dict) -> None:
    """Appelé par Airflow quand une tâche échoue après ses tentatives.

    Le message porte les trois informations dont on a besoin à 6 h du matin :
    quelle tâche, quelle date de traitement, et le lien direct vers les logs.
    """
    ti = context["task_instance"]
    exception = context.get("exception")

    message = (
        f"[ÉCHEC] {ti.dag_id}.{ti.task_id}\n"
        f"  date de traitement : {context['data_interval_start'].date()}\n"
        f"  tentative          : {ti.try_number - 1}/{ti.max_tries}\n"
        f"  cause              : {exception}\n"
        f"  logs               : {ti.log_url}"
    )
    logger.error(message)

    # Point de branchement pour une vraie notification. Exemple :
    # from airflow.providers.slack.notifications.slack import send_slack_notification
    # send_slack_notification(channel="#data-alertes", text=message)(context)


def alerter_ecarts(montant_eur: float, seuil_eur: float, jour: str) -> None:
    """Alerte métier : le chiffre d'affaires non expliqué dépasse le seuil.

    Ce n'est pas une erreur technique — le pipeline a fonctionné. C'est le
    résultat qui demande une action humaine, et le message s'adresse au métier.
    """
    message = (
        f"[ÉCART] Réconciliation du {jour} : "
        f"{montant_eur:,.2f} € restent à expliquer (seuil : {seuil_eur:,.2f} €). "
        f"Consulter gold_reconciliation, statuts ECART_SIGNIFICATIF, "
        f"ABSENT_ADSERVER et ABSENT_FACTURATION."
    ).replace(",", " ")
    logger.warning(message)
