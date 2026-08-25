# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze : ingestion incrémentale
# MAGIC
# MAGIC Auto Loader sur les deux dossiers, en `availableNow` : le stream traite ce qui est
# MAGIC présent puis s'arrête, ce qui convient à un job planifié.
# MAGIC
# MAGIC Règles de la couche bronze, tenues strictement :
# MAGIC
# MAGIC - **aucune transformation métier** — on conserve la donnée telle qu'elle arrive
# MAGIC - schéma explicite plutôt qu'inféré, pour que les valeurs illisibles tombent en
# MAGIC   `_rescued_data` au lieu de faire dériver le schéma silencieusement
# MAGIC - traçabilité : fichier d'origine et horodatage d'ingestion
# MAGIC
# MAGIC Le checkpoint porte l'incrémental : relancer le notebook ne réingère rien.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "reconciliation")
dbutils.widgets.text("volume", "landing")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CHECKPOINTS = f"{BASE}/_checkpoints"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

schema_source = StructType([
    StructField("campagne_id", StringType()),
    StructField("campagne_libelle", StringType()),
    StructField("placement_id", StringType()),
    StructField("date_diffusion", StringType()),
    StructField("impressions", IntegerType()),
    StructField("clics", IntegerType()),
    StructField("ca_eur", DoubleType()),
    StructField("source", StringType()),
    StructField("date_extraction", StringType()),
    StructField("_rescued_data", StringType()),
])

# COMMAND ----------

def ingerer(nom_source):
    """Ingère un dossier vers sa table bronze. Idempotent grâce au checkpoint."""
    table_cible = f"bronze_{nom_source}"

    flux = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{nom_source}/_schema")
        .schema(schema_source)
        .load(f"{BASE}/{nom_source}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_fichier_source"),
            F.current_timestamp().alias("_horodatage_ingestion"),
        )
    )

    (
        flux.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/{nom_source}/_commits")
        .trigger(availableNow=True)
        .toTable(table_cible)
        .awaitTermination()
    )

    n = spark.table(table_cible).count()
    print(f"{table_cible} : {n} lignes au total")
    return table_cible


for source in ("adserver", "facturation"):
    ingerer(source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle d'ingestion
# MAGIC
# MAGIC Un écart de volumétrie entre les deux tables est normal — c'est le sujet du projet.
# MAGIC Ce qui ne le serait pas : des lignes non nulles dans `_rescued_data`, qui signaleraient
# MAGIC un champ absent du schéma déclaré.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'adserver'                        AS source,
# MAGIC   COUNT(*)                          AS lignes,
# MAGIC   COUNT(DISTINCT date_diffusion)    AS jours,
# MAGIC   COUNT(DISTINCT _fichier_source)   AS fichiers,
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS lignes_rescapees
# MAGIC FROM bronze_adserver
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'facturation',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT date_diffusion),
# MAGIC   COUNT(DISTINCT _fichier_source),
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)
# MAGIC FROM bronze_facturation
