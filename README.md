# UrbanPulse – collecteur d'historique (GitHub Actions)

La page [Qualité de l'air à Rennes](https://www.airbreizh.asso.fr/ville/rennes/) d'Air Breizh
ne donne les **moyennes horaires que sur les 7 derniers jours**. Ce dépôt télécharge
**chaque semaine, pour chaque gaz, les 7 derniers jours de toutes les stations de Rennes**,
sur les serveurs de GitHub, que votre ordinateur soit allumé ou non. Semaine après
semaine, il constitue un historique horaire continu.

## Configuration actuelle : Air Breizh uniquement

| Fichier (source) | Gaz | Stations |
|---|---|---|
| `airbreizh_nh3` | NH3 | Mordelles |
| `airbreizh_no2` | NO2 | Halles, Laënnec, Thabor |
| `airbreizh_o3` | O3 | Mordelles, Thabor |
| `airbreizh_pm10` | PM10 | Laënnec, Thabor |
| `airbreizh_pm25` | PM2.5 | Laënnec, Thabor |

Chaque appel reproduit exactement le formulaire « Sélectionnez vos données » de la page
(toutes les stations cochées, un gaz, période « Moy. horaires des 7 derniers jours ») et
renvoie 169 valeurs horaires par station (7 jours + 1 h). Le workflow tourne toutes les
heures mais ne télécharge qu'une fois par semaine ; en cas d'échec, il retente à l'heure
suivante. La marge d'une à deux heures entre deux semaines évite les trous.

Les autres sources (trafic, STAR, travaux, vélos) sont présentes dans `sources.py` mais
**désactivées**. Pour en réactiver une : `"enabled": True`, puis remettre la planification
toutes les 5 minutes dans `.github/workflows/collect.yml`. **Sans la source `trafic`, aucun
historique de trafic n'est constitué** : c'est pourtant la cible du modèle de congestion.

## Mise en place (15 minutes, une seule fois)

1. **Créer un dépôt public** sur GitHub, par exemple `urbanpulse-collector`.
   Il doit être public : les exécutions GitHub Actions y sont gratuites et illimitées,
   alors qu'un dépôt privé dépasserait le quota gratuit (≈ 9 000 minutes par mois ici).
   Les données sont ouvertes (ODbL) : les publier ne pose pas de problème.
2. **Envoyer ces fichiers** dans le dépôt, dossier caché `.github/` compris :
   ```bash
   cd urbanpulse-collector
   git init -b main
   git add .
   git commit -m "Collecteur UrbanPulse"
   git remote add origin https://github.com/<votre-compte>/urbanpulse-collector.git
   git push -u origin main
   ```
   (Avec l'envoi par glisser-déposer sur le site, vérifier que `.github/workflows/collect.yml`
   est bien présent : certains systèmes masquent les dossiers commençant par un point.)
3. **Autoriser l'écriture** : *Settings → Actions → General → Workflow permissions* →
   cocher **Read and write permissions** → *Save*.
4. **Lancer un premier test** : onglet *Actions* → *Collecte UrbanPulse* → *Run workflow*.
   Au bout d'une minute environ, une branche `data` apparaît avec les 5 fichiers de la semaine.

C'est tout : le workflow se relance ensuite seul (toutes les heures, collecte hebdomadaire).

## Base de données Supabase (optionnel, recommandé)

À chaque exécution, les mesures nouvellement collectées sont aussi envoyées dans une base
**Supabase** (PostgreSQL), table `raw_mesures_air` : **une ligne par gaz, par station et par heure**.
Les fichiers bruts et les Releases GitHub restent la copie de référence.

Mise en place (une seule fois) :

1. Créer un projet sur https://supabase.com (offre gratuite), région **Europe (Paris ou Francfort)**.
2. **SQL Editor → New query** : coller le contenu de `supabase_schema.sql`, puis **Run**.
   Cela crée les tables `raw_mesures_air` et `raw_ingestion_log` et la vue `v_no2_ecart_trafic`.
3. Récupérer deux valeurs dans Supabase, **Project Settings** :
   - **Data API** : l'URL du projet (`https://xxxx.supabase.co`) ;
   - **API Keys** : la clé **secrète** (`sb_secret_…`, ou l'ancienne clé `service_role`).
4. Sur GitHub : **Settings → Secrets and variables → Actions → New repository secret**, créer :
   - `SUPABASE_URL` = l'URL du projet ;
   - `SUPABASE_SERVICE_KEY` = la clé secrète.
   Ne jamais écrire cette clé dans un fichier du dépôt : le dépôt est public.
5. Onglet **Actions → Run workflow** : les données de la semaine apparaissent dans
   **Table Editor → raw_mesures_air**.

Fonctionnement :
- clé de la table : (polluant, station, heure). Une collecte plus récente met à jour l'heure
  (valeur validée par Air Breizh entre-temps) ; une heure vide n'écrase jamais une valeur ;
- chaque fichier n'est envoyé qu'une fois (`data/raw/_db_loaded.json`) ; si Supabase est
  indisponible, l'envoi est retenté à l'exécution suivante, sans bloquer l'archivage GitHub ;
- sécurité : Row Level Security activé sans règle publique, seule la clé secrète peut écrire ;
- rattrapage depuis les Releases si besoin :
  `python download_history.py MH475/lanceur_pollution` puis
  `SUPABASE_URL=… SUPABASE_SERVICE_KEY=… python load_supabase.py --history history`.

Lire les données en Python (entraînement, Streamlit) : chaîne de connexion dans
**Connect → Session pooler**, puis
`pd.read_sql("select * from raw_mesures_air", "postgresql://…")`.

## Où sont les données

| Emplacement | Contenu |
|---|---|
| Branche **`data`** | Les réponses brutes **du jour en cours** (`raw/<source>/date=…/`) et le journal du jour (`raw/_logs/`) |
| **Releases** `data-AAAA-MM` (une par mois) | L'historique : un fichier Parquet **par gaz et par semaine** (`airbreizh_no2__2026-09-25.parquet`…, daté du jour de collecte) + le journal des appels |

Chaque nuit (après minuit UTC), la journée terminée est compactée en Parquet et publiée
dans la Release du mois. La branche `data` repart ensuite d'un historique vide pour que
le dépôt reste léger. Si la publication échoue, les données restent sur la branche et
seront republiées à l'exécution suivante : rien n'est supprimé avant d'être publié.

## Contenu des fichiers Parquet Air Breizh

Une ligne par station et par heure : `polluant`, `station_code`, `station`, `id_mesure`,
`date_utc`, `date_local`, `valeur_ugm3`, `validated`, `count`, plus `_collected_at_utc`
et `_raw_file` (traçabilité). Les heures sans mesure (`valeur_ugm3` vide) sont conservées :
elles signalent un trou de mesure chez Air Breizh.

## Récupérer l'historique pour l'entraînement

Sur votre PC :

```bash
pip install -r requirements.txt
python download_history.py <votre-compte>/urbanpulse-collector
```

```python
import glob, pandas as pd
air = pd.concat(pd.read_parquet(p) for p in glob.glob("history/bronze/airbreizh_*/*/*.parquet"))
air["date_utc"] = pd.to_datetime(air["date_utc"])
# Les semaines se chevauchent d'1 à 2 h : garder la collecte la plus récente de chaque heure
air = (air.sort_values("_collected_at_utc")
          .drop_duplicates(["polluant", "station_code", "date_utc"], keep="last"))
```

À savoir avant d'utiliser les valeurs :
- les heures récentes sont **provisoires** (`validated` à 0) ; Air Breizh les valide dans
  les 2 mois. Une valeur collectée reste celle du jour de collecte : signaler cette limite ;
- repérer les **trous** (heures sans valeur, semaines non collectées) grâce aux journaux
  (`history/logs/`), sans les combler en silence.

## Surveiller que ça tourne

- Onglet *Actions* : chaque exécution doit être verte. En cas d'échec répété, GitHub envoie
  un e-mail au propriétaire du dépôt.
- Journal du jour sur la branche `data` : `raw/_logs/ingestion_log_<date>.csv`
  (statut HTTP, nombre d'enregistrements, fraîcheur de la donnée source, erreur éventuelle).
- Le lendemain de la première collecte, vérifier qu'une Release `data-AAAA-MM` est apparue.
- Si Air Breizh modifie sa page, l'appel peut changer : la colonne `error` du journal le
  signale (« Réponse inattendue d'Air Breizh »). Les codes des stations et des gaz sont dans
  `sources.py`.

## Limites à connaître (et à citer dans le dossier)

- **Source non officielle** : l'adresse utilisée est celle qu'appelle la page web d'Air Breizh
  (pas une API documentée). Elle peut changer sans préavis ; le portail open data
  d'Air Breizh (WFS) reste l'accès officiel à citer dans le dossier.
- **Ponctualité** : GitHub peut retarder ou sauter des exécutions planifiées. Le workflow
  retente toutes les heures : il faudrait plus d'une heure d'échecs consécutifs le jour de la
  collecte pour perdre des heures de mesure, et ce serait visible dans les journaux.
- **Inactivité** : GitHub désactive les workflows planifiés d'un dépôt sans activité depuis
  60 jours. Le workflow se réactive lui-même chaque nuit ; en cas de doute, vérifier l'onglet
  *Actions* une fois par mois.
- **Adresses des sources** : si une adresse change (flux vélos, identifiants des jeux travaux),
  la colonne `error` du journal le signale ; il suffit de la corriger dans `sources.py`.

## Fichiers

| Fichier | Rôle |
|---|---|
| `.github/workflows/collect.yml` | Planification toutes les 5 min, sauvegarde sur la branche `data` |
| `sources.py` | Liste des sources, adresses et fréquences |
| `collect.py` | Collecte et archivage brut + journal (`--due`, `--all`, `--loop`) |
| `compact.py` | Compaction d'une journée en Parquet |
| `ci_nightly.py` | Publication nocturne dans la Release du mois |
| `download_history.py` | Rapatriement de l'historique sur votre PC |
| `supabase_schema.sql` | Tables Supabase à créer une fois |
| `load_supabase.py` | Envoi des mesures dans Supabase |
| `crontab.example`, `Dockerfile`, `run.sh` | Alternative : faire tourner sur une VM |

Licences : données Rennes Métropole et STAR sous ODbL. Citer la source et partager toute
base dérivée sous la même licence.
