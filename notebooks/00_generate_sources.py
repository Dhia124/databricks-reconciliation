# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Génération des deux sources
# MAGIC
# MAGIC Simule deux systèmes censés porter les mêmes chiffres de diffusion publicitaire :
# MAGIC
# MAGIC - **`adserver`** : remontée J-1, partielle et non consolidée
# MAGIC - **`facturation`** : mêmes campagnes, chiffres révisés jusqu'à J+8
# MAGIC
# MAGIC La divergence est volontaire et paramétrée : révisions tardives, lignes absentes
# MAGIC d'un côté, doublons, valeurs aberrantes. C'est ce que le pipeline devra détecter.
# MAGIC
# MAGIC Les fichiers sont écrits un jour à la fois pour qu'Auto Loader ait de l'incrémental
# MAGIC réel à traiter au notebook suivant.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "reconciliation")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("nb_jours", "30")
dbutils.widgets.text("graine", "42")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
NB_JOURS = int(dbutils.widgets.get("nb_jours"))
GRAINE = int(dbutils.widgets.get("graine"))

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"Zone d'atterrissage : {BASE}")

# COMMAND ----------

import json
import random
from datetime import date, timedelta, datetime

random.seed(GRAINE)

CAMPAGNES = [
    ("CMP-1001", "Assurance Auto - Display", 12),
    ("CMP-1002", "Banque - Video Preroll", 8),
    ("CMP-1003", "Telecom - Native", 15),
    ("CMP-1004", "Retail - Display Mobile", 10),
    ("CMP-1005", "Energie - Video Outstream", 6),
]

DATE_FIN = date.today() - timedelta(days=1)
DATE_DEBUT = DATE_FIN - timedelta(days=NB_JOURS - 1)


def lignes_du_jour(jour):
    """Vérité terrain d'une journée : une ligne par placement actif."""
    lignes = []
    for campagne_id, libelle, nb_placements in CAMPAGNES:
        for i in range(1, nb_placements + 1):
            if random.random() < 0.15:          # placement inactif ce jour-là
                continue
            impressions = random.randint(5_000, 250_000)
            taux_clic = random.uniform(0.0008, 0.019)
            cpm = random.uniform(2.5, 18.0)
            lignes.append({
                "campagne_id": campagne_id,
                "campagne_libelle": libelle,
                "placement_id": f"{campagne_id}-P{i:03d}",
                "date_diffusion": jour.isoformat(),
                "impressions": impressions,
                "clics": int(impressions * taux_clic),
                "ca_eur": round(impressions / 1000 * cpm, 2),
            })
    return lignes


def variante_adserver(ligne):
    """Remontée J-1 : sous-comptage systématique, quelques lignes manquantes."""
    if random.random() < 0.03:                   # 3 % jamais remontées
        return None
    l = dict(ligne)
    facteur = random.uniform(0.94, 1.0)          # consolidation à venir
    l["impressions"] = int(l["impressions"] * facteur)
    l["clics"] = int(l["clics"] * facteur)
    l["ca_eur"] = round(l["ca_eur"] * facteur, 2)
    l["source"] = "adserver"
    return l


def variante_facturation(ligne):
    """Chiffres consolidés, avec quelques anomalies de traitement."""
    if random.random() < 0.02:                   # 2 % absentes côté facturation
        return None
    l = dict(ligne)
    if random.random() < 0.015:                  # erreur de saisie manifeste
        l["ca_eur"] = round(l["ca_eur"] * random.choice([10.0, 0.1]), 2)
    if random.random() < 0.01:                   # valeur négative aberrante
        l["impressions"] = -abs(l["impressions"])
    l["source"] = "facturation"
    return l

# COMMAND ----------

def ecrire(chemin_dossier, nom_fichier, enregistrements):
    dbutils.fs.mkdirs(chemin_dossier)
    contenu = "\n".join(json.dumps(e, ensure_ascii=False) for e in enregistrements)
    dbutils.fs.put(f"{chemin_dossier}/{nom_fichier}", contenu, overwrite=True)


total_a = total_b = 0
jour = DATE_DEBUT

while jour <= DATE_FIN:
    verite = lignes_du_jour(jour)
    horodatage = datetime.now().isoformat(timespec="seconds")

    lot_a = [v for v in (variante_adserver(l) for l in verite) if v]
    lot_b = [v for v in (variante_facturation(l) for l in verite) if v]

    # doublon occasionnel côté facturation : le pipeline devra le neutraliser
    if lot_b and random.random() < 0.2:
        lot_b.append(dict(random.choice(lot_b)))

    for l in lot_a + lot_b:
        l["date_extraction"] = horodatage

    ecrire(f"{BASE}/adserver", f"adserver_{jour.isoformat()}.json", lot_a)
    ecrire(f"{BASE}/facturation", f"facturation_{jour.isoformat()}.json", lot_b)

    total_a += len(lot_a)
    total_b += len(lot_b)
    jour += timedelta(days=1)

print(f"adserver    : {total_a} lignes sur {NB_JOURS} fichiers")
print(f"facturation : {total_b} lignes sur {NB_JOURS} fichiers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rejouer de l'incrémental
# MAGIC
# MAGIC Les deux volumétries diffèrent : c'est attendu, et c'est le point de départ du projet.
# MAGIC
# MAGIC Après avoir exécuté les notebooks 01 à 03, relancez **uniquement la cellule ci-dessous**
# MAGIC pour déposer une journée supplémentaire. Auto Loader ne reprendra que le nouveau fichier :
# MAGIC c'est la démonstration que l'ingestion est bien incrémentale.

# COMMAND ----------

nouveau_jour = DATE_FIN + timedelta(days=1)
verite = lignes_du_jour(nouveau_jour)
horodatage = datetime.now().isoformat(timespec="seconds")

lot_a = [v for v in (variante_adserver(l) for l in verite) if v]
lot_b = [v for v in (variante_facturation(l) for l in verite) if v]
for l in lot_a + lot_b:
    l["date_extraction"] = horodatage

ecrire(f"{BASE}/adserver", f"adserver_{nouveau_jour.isoformat()}.json", lot_a)
ecrire(f"{BASE}/facturation", f"facturation_{nouveau_jour.isoformat()}.json", lot_b)

print(f"Journée supplémentaire déposée : {nouveau_jour}")
