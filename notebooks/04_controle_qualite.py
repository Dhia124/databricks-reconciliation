# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Contrôle de fin de chaîne
# MAGIC
# MAGIC Ce notebook ne transforme rien. Il répond à une seule question, celle que
# MAGIC l'orchestrateur doit trancher : **combien d'euros restent inexpliqués pour la
# MAGIC journée traitée ?**
# MAGIC
# MAGIC Il vérifie au passage qu'aucune clé métier ne s'est perdue entre silver et
# MAGIC gold, puis renvoie son résultat à Airflow via `dbutils.notebook.exit`. C'est
# MAGIC cette valeur qui déclenche — ou non — l'alerte au métier.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "reconciliation")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# Contrôle de cohérence : toute clé présente en silver doit exister en gold.
cles_silver = spark.sql("""
    SELECT COUNT(*) FROM (
        SELECT DISTINCT campagne_id, placement_id, date_diffusion
        FROM silver_diffusion
    )
""").collect()[0][0]

lignes_gold = spark.table("gold_reconciliation").count()

assert cles_silver == lignes_gold, (
    f"Perte de lignes entre silver et gold : {cles_silver} clés en silver, "
    f"{lignes_gold} en gold. La jointure a un problème de clé."
)

# COMMAND ----------

# Le jour le plus récent présent dans la synthèse fait foi.
ligne = spark.sql("""
    SELECT
        date_diffusion,
        ca_a_expliquer_eur,
        ecarts_significatifs,
        absents_adserver,
        absents_facturation
    FROM gold_synthese_quotidienne
    ORDER BY date_diffusion DESC
    LIMIT 1
""").collect()

if not ligne:
    raise ValueError("gold_synthese_quotidienne est vide : le pipeline n'a rien produit.")

r = ligne[0]
resultat = {
    "jour": str(r["date_diffusion"]),
    "ca_a_expliquer_eur": float(r["ca_a_expliquer_eur"] or 0.0),
    "ecarts_significatifs": int(r["ecarts_significatifs"] or 0),
    "lignes_absentes": int((r["absents_adserver"] or 0) + (r["absents_facturation"] or 0)),
    "cles_verifiees": int(lignes_gold),
}

print(json.dumps(resultat, indent=2))

# COMMAND ----------

# C'est cette sortie qu'Airflow lit pour décider s'il faut alerter.
dbutils.notebook.exit(json.dumps(resultat))
