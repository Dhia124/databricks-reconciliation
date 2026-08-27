# Ajouter l'orchestration au dépôt `databricks-reconciliation`

Ce paquet ne contient **que les fichiers nouveaux**. Vos notebooks `00` à `03`
ne sont pas dedans : ils ne changent pas.

## 1. Copier les fichiers

Dézippez ce paquet à côté de votre dépôt, puis, depuis la racine du dépôt :

```bash
cd ~/chemin/vers/databricks-reconciliation

cp -r ~/Téléchargements/ajout-airflow/dags .
cp    ~/Téléchargements/ajout-airflow/notebooks/04_controle_qualite.py notebooks/
cp    ~/Téléchargements/ajout-airflow/docker-compose.yaml .
cp    ~/Téléchargements/ajout-airflow/.env.example .
```

Arborescence attendue après copie :

```
databricks-reconciliation/
├── README.md
├── docker-compose.yaml          ← nouveau
├── .env.example                 ← nouveau
├── .gitignore                   ← à créer ou compléter (étape 2)
├── dags/                        ← nouveau dossier
│   ├── .airflowignore
│   ├── reconciliation_quotidienne.py
│   └── utils/
│       ├── __init__.py
│       └── alertes.py
└── notebooks/
    ├── 00_generate_sources.py   ← inchangé
    ├── 01_bronze.py             ← inchangé
    ├── 02_silver.py             ← inchangé
    ├── 03_gold_reconciliation.py ← inchangé
    └── 04_controle_qualite.py   ← nouveau
```

## 2. Le `.gitignore` — l'étape à ne pas sauter

Ouvrez `gitignore-a-fusionner.txt` et collez son contenu à la fin de votre
`.gitignore`. S'il n'existe pas encore :

```bash
cp ~/Téléchargements/ajout-airflow/gitignore-a-fusionner.txt .gitignore
```

Pourquoi c'est la ligne la plus importante du fichier : **`.env`**.
Une fois Airflow configuré en mode Databricks, ce fichier contiendra votre token
d'accès personnel. Un token poussé sur un dépôt public donne à n'importe qui
l'accès à votre workspace. Le fichier `.env.example`, lui, ne contient aucun
secret — c'est celui qu'on versionne.

`logs/` compte aussi : Airflow y écrit à chaque exécution de tâche, et ce dossier
grossit vite.

## 3. Vérifier AVANT de valider

```bash
git status
```

Lisez la liste. Vous devez y voir `dags/`, `docker-compose.yaml`, `.env.example`,
`notebooks/04_controle_qualite.py`, `.gitignore`.

Vous ne devez **jamais** y voir `.env`, `logs/`, `__pycache__/`,
`standalone_admin_password.txt`. Si l'un d'eux apparaît, le `.gitignore` n'est
pas au bon endroit ou pas encore enregistré — corrigez avant de continuer.

## 4. Valider et pousser

```bash
git add .
git commit -m "Ajout de l'orchestration Airflow du pipeline de réconciliation

- DAG quotidien à six tâches enchaînant les notebooks 00 à 04
- Branchement métier sur le seuil de CA restant à expliquer
- Notebook 04 de contrôle de fin de chaîne renvoyant son résultat à Airflow
- Environnement Airflow local via docker compose"
git push
```

## 5. Compléter le README

Le fichier `README-section-a-ajouter.md` contient une section prête à coller à la
fin de votre `README.md` actuel. Elle explique l'orchestration dans le même
esprit que le reste du document.

## Si quelque chose casse

**`ModuleNotFoundError: No module named 'utils'`** au démarrage d'Airflow — le
dossier `dags/utils/` n'a pas été copié entièrement, ou `__init__.py` manque.

**Le DAG n'apparaît pas dans l'interface** — attendez trente secondes, Airflow
scanne le dossier périodiquement. S'il n'apparaît toujours pas, l'onglet
*Browse → DAG Import Errors* affiche la raison exacte.

**Le port 8080 est déjà pris** — changez `"8080:8080"` en `"8081:8080"` dans
`docker-compose.yaml`, puis ouvrez `localhost:8081`.

**Vous avez déjà poussé un `.env` par erreur** — le retirer du prochain commit ne
suffit pas, il reste dans l'historique. Révoquez le token dans Databricks
immédiatement, puis nettoyez l'historique. Un token révoqué ne vaut plus rien,
c'est la seule action qui compte vraiment.
