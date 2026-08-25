# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold : réconciliation et écarts
# MAGIC
# MAGIC Le cœur du projet. On pivote les deux sources côte à côte sur la clé métier commune,
# MAGIC puis on qualifie chaque ligne.
# MAGIC
# MAGIC **Le `FULL OUTER JOIN` n'est pas un détail de style.** Un `INNER JOIN` ne verrait que
# MAGIC les lignes présentes des deux côtés — c'est-à-dire qu'il masquerait exactement les
# MAGIC anomalies les plus graves : une diffusion facturée mais jamais remontée par l'adserver,
# MAGIC ou l'inverse. Les lignes manquantes d'un côté sont le premier motif d'écart, pas un
# MAGIC cas limite.
# MAGIC
# MAGIC Le seuil de tolérance est paramétré : en dessous, l'écart relève de l'arrondi et du
# MAGIC délai de consolidation ; au-dessus, il demande une explication.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "reconciliation")
dbutils.widgets.text("seuil_ca_pct", "2.0")
dbutils.widgets.text("seuil_ca_abs_eur", "50.0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEUIL_PCT = float(dbutils.widgets.get("seuil_ca_pct"))
SEUIL_ABS = float(dbutils.widgets.get("seuil_ca_abs_eur"))

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Tolérance : {SEUIL_PCT} % ET {SEUIL_ABS} € — un écart doit franchir les deux pour être signalé")

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("silver_diffusion")
CLES = ["campagne_id", "placement_id", "date_diffusion"]

adserver = (
    silver.filter(F.col("source") == "adserver")
          .select(*CLES,
                  F.col("campagne_libelle").alias("libelle_adserver"),
                  F.col("impressions").alias("impressions_adserver"),
                  F.col("ca_eur").alias("ca_adserver_eur"))
)

facturation = (
    silver.filter(F.col("source") == "facturation")
          .select(*CLES,
                  F.col("campagne_libelle").alias("libelle_facturation"),
                  F.col("impressions").alias("impressions_facturation"),
                  F.col("ca_eur").alias("ca_facturation_eur"))
)

# COMMAND ----------

joint = adserver.join(facturation, on=CLES, how="full_outer")

ecart_ca = F.col("ca_facturation_eur") - F.col("ca_adserver_eur")
base_pct = F.greatest(F.abs(F.col("ca_adserver_eur")), F.lit(0.01))

reconciliation = (
    joint
    # le libellé vient de l'un ou l'autre côté selon la ligne manquante
    .withColumn("campagne_libelle",
                F.coalesce("libelle_adserver", "libelle_facturation"))
    .withColumn("ecart_ca_eur", F.round(ecart_ca, 2))
    .withColumn("ecart_ca_pct", F.round(ecart_ca / base_pct * 100, 2))
    .withColumn("ecart_impressions",
                F.col("impressions_facturation") - F.col("impressions_adserver"))
    .withColumn(
        "statut",
        F.when(F.col("ca_adserver_eur").isNull(), "ABSENT_ADSERVER")
         .when(F.col("ca_facturation_eur").isNull(), "ABSENT_FACTURATION")
         .when(
             (F.abs(F.col("ecart_ca_pct")) > SEUIL_PCT)
             & (F.abs(F.col("ecart_ca_eur")) > SEUIL_ABS),
             "ECART_SIGNIFICATIF",
         )
         .when(F.abs(F.col("ecart_ca_eur")) > 0.01, "ECART_TOLERE")
         .otherwise("CONFORME"),
    )
    .withColumn("_date_calcul", F.current_timestamp())
    .select(
        "campagne_id", "campagne_libelle", "placement_id", "date_diffusion",
        "impressions_adserver", "impressions_facturation", "ecart_impressions",
        "ca_adserver_eur", "ca_facturation_eur", "ecart_ca_eur", "ecart_ca_pct",
        "statut", "_date_calcul",
    )
)

(reconciliation.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable("gold_reconciliation"))

print(f"gold_reconciliation : {spark.table('gold_reconciliation').count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthèse quotidienne
# MAGIC
# MAGIC C'est cette table que consomme le tableau de bord. Elle porte l'indicateur qui
# MAGIC intéresse le métier : **combien d'euros ne sont pas expliqués aujourd'hui.**

# COMMAND ----------

synthese = spark.sql("""
    SELECT
        date_diffusion,
        COUNT(*)                                                       AS lignes,
        SUM(CASE WHEN statut = 'CONFORME'           THEN 1 ELSE 0 END) AS conformes,
        SUM(CASE WHEN statut = 'ECART_TOLERE'       THEN 1 ELSE 0 END) AS ecarts_toleres,
        SUM(CASE WHEN statut = 'ECART_SIGNIFICATIF' THEN 1 ELSE 0 END) AS ecarts_significatifs,
        SUM(CASE WHEN statut = 'ABSENT_ADSERVER'    THEN 1 ELSE 0 END) AS absents_adserver,
        SUM(CASE WHEN statut = 'ABSENT_FACTURATION' THEN 1 ELSE 0 END) AS absents_facturation,
        ROUND(SUM(COALESCE(ca_adserver_eur, 0)), 2)                    AS ca_adserver_eur,
        ROUND(SUM(COALESCE(ca_facturation_eur, 0)), 2)                 AS ca_facturation_eur,
        ROUND(SUM(COALESCE(ca_facturation_eur, 0))
            - SUM(COALESCE(ca_adserver_eur, 0)), 2)                    AS ecart_total_eur,
        ROUND(SUM(CASE WHEN statut IN ('ECART_SIGNIFICATIF', 'ABSENT_ADSERVER', 'ABSENT_FACTURATION')
                       THEN ABS(COALESCE(ca_facturation_eur, 0) - COALESCE(ca_adserver_eur, 0))
                       ELSE 0 END), 2)                                 AS ca_a_expliquer_eur
    FROM gold_reconciliation
    GROUP BY date_diffusion
    ORDER BY date_diffusion DESC
""")

(synthese.write.mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("gold_synthese_quotidienne"))

spark.table("gold_synthese_quotidienne").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les écarts à traiter en priorité
# MAGIC
# MAGIC Classés par montant, pas par ancienneté : c'est ainsi que le métier arbitre.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_diffusion, campagne_libelle, placement_id, statut,
# MAGIC   ca_adserver_eur, ca_facturation_eur, ecart_ca_eur, ecart_ca_pct
# MAGIC FROM gold_reconciliation
# MAGIC WHERE statut IN ('ECART_SIGNIFICATIF', 'ABSENT_ADSERVER', 'ABSENT_FACTURATION')
# MAGIC ORDER BY ABS(ecart_ca_eur) DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle de cohérence du pipeline
# MAGIC
# MAGIC Aucune clé métier ne doit se perdre entre silver et gold. Si ce test échoue,
# MAGIC la jointure a un problème de clé — c'est le premier endroit où regarder.

# COMMAND ----------

cles_silver = spark.sql("""
    SELECT COUNT(*) FROM (
        SELECT DISTINCT campagne_id, placement_id, date_diffusion
        FROM silver_diffusion
    )
""").collect()[0][0]

lignes_gold = spark.table("gold_reconciliation").count()

assert cles_silver == lignes_gold, (
    f"Perte de lignes : {cles_silver} clés en silver, {lignes_gold} en gold"
)
print(f"Cohérence vérifiée : {lignes_gold} clés métier de bout en bout")
