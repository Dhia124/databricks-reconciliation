# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver : nettoyage, qualité, quarantaine
# MAGIC
# MAGIC Trois responsabilités, dans cet ordre :
# MAGIC
# MAGIC 1. **Typage et normalisation** — `date_diffusion` devient une vraie date, les libellés
# MAGIC    sont nettoyés
# MAGIC 2. **Déduplication** — la source facturation produit des doublons ; on garde la
# MAGIC    dernière extraction par clé métier
# MAGIC 3. **Contrôles qualité** — les lignes non conformes ne sont pas supprimées, elles sont
# MAGIC    **mises en quarantaine avec leur motif**
# MAGIC
# MAGIC Ce dernier point est celui qui compte. Jeter une ligne invalide fait disparaître le
# MAGIC problème sans le résoudre ; la mettre en quarantaine avec un motif la rend
# MAGIC exploitable par l'équipe métier, qui peut corriger à la source.
# MAGIC
# MAGIC > Ce notebook utilise du PySpark simple, qui fonctionne partout. Si Delta Live Tables
# MAGIC > est disponible sur votre workspace, la même logique s'écrit avec `@dlt.expect_or_drop`
# MAGIC > et une table de quarantaine — c'est un bon deuxième exercice.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "reconciliation")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F, Window

# Les règles sont déclaratives : une condition, un motif. En ajouter une se fait ici,
# et nulle part ailleurs dans le notebook.
REGLES_QUALITE = [
    ("impressions_negatives",        F.col("impressions") < 0),
    ("ca_negatif",                   F.col("ca_eur") < 0),
    ("clics_superieurs_impressions", F.col("clics") > F.col("impressions")),
    ("cle_metier_incomplete",        F.col("campagne_id").isNull()
                                     | F.col("placement_id").isNull()
                                     | F.col("date_diffusion").isNull()),
    ("cpm_aberrant",                 (F.col("impressions") > 0)
                                     & ((F.col("ca_eur") / F.col("impressions") * 1000) > 100)),
]

# COMMAND ----------

def preparer(table_bronze):
    """Typage, normalisation, déduplication sur la clé métier."""
    df = (
        spark.table(table_bronze)
        .withColumn("date_diffusion", F.to_date("date_diffusion", "yyyy-MM-dd"))
        .withColumn("date_extraction", F.to_timestamp("date_extraction"))
        .withColumn("campagne_libelle", F.trim("campagne_libelle"))
    )

    # Dernière extraction connue pour chaque (source, campagne, placement, jour)
    fenetre = Window.partitionBy(
        "source", "campagne_id", "placement_id", "date_diffusion"
    ).orderBy(F.col("date_extraction").desc(), F.col("_horodatage_ingestion").desc())

    return (
        df.withColumn("_rang", F.row_number().over(fenetre))
          .filter(F.col("_rang") == 1)
          .drop("_rang")
    )


prepare = preparer("bronze_adserver").unionByName(
    preparer("bronze_facturation"), allowMissingColumns=True
)

print(f"Lignes après déduplication : {prepare.count()}")

# COMMAND ----------

# Un motif par règle violée, concaténés : une ligne peut échouer sur plusieurs contrôles.
motifs = F.array_compact(F.array(*[
    F.when(condition, F.lit(nom)) for nom, condition in REGLES_QUALITE
]))

evalue = prepare.withColumn("_motifs", motifs)

colonnes = [
    "source", "campagne_id", "campagne_libelle", "placement_id",
    "date_diffusion", "impressions", "clics", "ca_eur", "_horodatage_ingestion",
]

conforme = evalue.filter(F.size("_motifs") == 0).select(*colonnes)

quarantaine = (
    evalue.filter(F.size("_motifs") > 0)
          .withColumn("motif_rejet", F.concat_ws(", ", "_motifs"))
          .select(*colonnes, "motif_rejet")
)

# COMMAND ----------

(conforme.write.mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("silver_diffusion"))

(quarantaine.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver_quarantaine"))

n_ok = spark.table("silver_diffusion").count()
n_ko = spark.table("silver_quarantaine").count()
taux = n_ko / (n_ok + n_ko) * 100 if (n_ok + n_ko) else 0

print(f"silver_diffusion   : {n_ok} lignes conformes")
print(f"silver_quarantaine : {n_ko} lignes rejetées ({taux:.2f} %)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ce que la quarantaine raconte
# MAGIC
# MAGIC La répartition des motifs est l'indicateur à suivre dans le temps. Un motif qui
# MAGIC progresse signale une dégradation à la source, pas un problème de pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source,
# MAGIC   motif_rejet,
# MAGIC   COUNT(*)                AS lignes,
# MAGIC   ROUND(SUM(ca_eur), 2)   AS ca_concerne_eur,
# MAGIC   MIN(date_diffusion)     AS premiere_occurrence,
# MAGIC   MAX(date_diffusion)     AS derniere_occurrence
# MAGIC FROM silver_quarantaine
# MAGIC GROUP BY source, motif_rejet
# MAGIC ORDER BY lignes DESC
