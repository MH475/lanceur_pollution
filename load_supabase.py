"""
UrbanPulse - chargement des collectes Air Breizh dans Supabase (architecture médaillon).

    fichier brut .json.gz  --->  BRONZE bronze.airbreizh_raw (réponse telle quelle, jsonb)
                                    |  transformation SQL dans la base
                                    v
                                 SILVER silver.mesures_air (typée, dédoublonnée, contrôlée)
                                    |  vues
                                    v
                                 GOLD   gold.* (indicateurs dashboard / modèle)

Ce script ne fait que la première flèche : il envoie chaque fichier brut non encore
chargé à la fonction public.ingest_airbreizh(), qui l'écrit en bronze puis met à jour
silver. Toute la logique de nettoyage est en SQL (supabase_medallion.sql), donc
rejouable : `select silver.rebuild();` reconstruit silver depuis bronze.

- Chaque fichier n'est envoyé qu'une fois (data/raw/_bronze_loaded.json) ; en cas
  d'erreur, il est retenté à l'exécution suivante.
- Le journal de collecte est envoyé dans bronze.ingestion_log (public.ingest_log()).
- Ne fait jamais échouer le workflow : une panne de Supabase ne doit pas empêcher
  l'archivage des données sur GitHub. Dernier résultat : data/raw/_db_status.json.

Variables d'environnement (secrets GitHub) :
    SUPABASE_URL          ex. https://abcdefgh.supabase.co
    SUPABASE_SERVICE_KEY  clé secrète (sb_secret_... ou service_role)

Usage :
    python load_supabase.py                        # fichiers bruts de data/raw
    python load_supabase.py --history history      # rattrapage depuis les Parquet des Releases
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"
STATE_FILE = RAW_DIR / "_bronze_loaded.json"
STATUS_FILE = RAW_DIR / "_db_status.json"     # dernier résultat, lisible sur la branche data
TIMEOUT_S = 120
ERRORS: list[str] = []

log = logging.getLogger("supabase")


class Supabase:
    def __init__(self, url: str, key: str):
        # Seule l'adresse du projet compte : tolère une URL copiée avec /rest/v1 ou un chemin
        u = urlparse(url if "://" in url else f"https://{url}")
        self.base = f"{u.scheme}://{u.netloc}/rest/v1"
        self.session = requests.Session()
        self.session.headers.update({"apikey": key, "Content-Type": "application/json"})
        if key.startswith("eyJ"):                 # ancienne clé service_role (JWT)
            self.session.headers["Authorization"] = f"Bearer {key}"

    def rpc(self, function: str, args: dict):
        r = self.session.post(f"{self.base}/rpc/{function}", data=json.dumps(args), timeout=TIMEOUT_S)
        if r.status_code >= 300:
            raise RuntimeError(f"{function} : HTTP {r.status_code} {r.text[:300]}")
        return r.json() if r.content else None


# --------------------------------------------------------------------------- état local
def load_state() -> dict[str, str]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, str]) -> None:
    limit = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    state = {k: v for k, v in state.items() if v >= limit}          # l'état ne grossit pas
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")


def polluant_of(source: str) -> str:
    from sources import SOURCES
    return SOURCES[source]["polluant"]


def ingest(db: Supabase, source: str, collected_at: str, raw_file: str, content: bytes) -> dict:
    return db.rpc("ingest_airbreizh", {
        "p_source": source,
        "p_polluant": polluant_of(source),
        "p_collected_at": collected_at,
        "p_raw_file": raw_file,
        "p_sha256": hashlib.sha256(content).hexdigest(),
        "p_payload": json.loads(content),
    })


# --------------------------------------------------------------------------- chargements
def load_raw(db: Supabase) -> int:
    state = load_state()
    errors = 0
    for path in sorted(RAW_DIR.glob("airbreizh_*/date=*/*.json.gz")):
        key = str(path.relative_to(RAW_DIR))
        if key in state:
            continue
        try:
            stamp = path.name.split("_")[-1].split(".")[0]              # 20260925T130933Z
            collected_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
            with gzip.open(path, "rb") as f:
                content = f.read()
            res = ingest(db, path.parent.parent.name, collected_at, f"raw/{key}", content)
            state[key] = datetime.now(timezone.utc).isoformat()
            log.info("%s -> bronze #%s (nouveau : %s), %s lignes silver",
                     key, res.get("bronze_id"), res.get("nouveau"), res.get("lignes_silver"))
        except Exception as exc:
            errors += 1
            log.error("%s : %s (retenté à la prochaine exécution)", key, exc)
            ERRORS.append(f"{key} : {exc}")
    save_state(state)
    return errors


def load_logs(db: Supabase, log_dir: Path) -> int:
    ints = {"http_status", "bytes", "n_records", "duration_ms"}
    rows = []
    for path in sorted(log_dir.glob("ingestion_log*.csv")):
        with path.open(encoding="utf-8") as f:
            for r in csv.DictReader(f):
                row = {k: (v if v != "" else None) for k, v in r.items()}
                row["collected_at"] = row.pop("collected_at_utc")
                for k in ints:
                    row[k] = int(row[k]) if row.get(k) is not None else None
                row["stored"] = str(r.get("stored")).lower() == "true"
                rows.append(row)
    if not rows:
        return 0
    try:
        db.rpc("ingest_log", {"p_rows": rows})
        log.info("Journal : %d ligne(s) envoyée(s)", len(rows))
        return 0
    except Exception as exc:
        log.error("Journal non envoyé : %s", exc)
        ERRORS.append(f"ingestion_log : {exc}")
        return 1


def load_history(db: Supabase, history: Path) -> int:
    """Rattrapage depuis les Parquet des Releases : on reconstitue, pour chaque fichier
    brut d'origine, une réponse au format Air Breizh, envoyée en bronze comme les autres."""
    import pandas as pd
    errors = 0
    frames = [pd.read_parquet(p) for p in sorted(history.glob("bronze/airbreizh_*/date=*/part-0.parquet"))]
    if not frames:
        log.info("Aucun Parquet Air Breizh dans %s", history)
        return load_logs(db, history / "logs")
    df = pd.concat(frames, ignore_index=True)
    df = df.astype(object).where(df.notna(), None)
    for raw_file, g in df.groupby("_raw_file"):
        try:
            payload: dict[str, list] = {}
            for r in g.to_dict("records"):
                payload.setdefault(r["station_code"], []).append({
                    "date_utc": r["date_utc"], "date_local": r["date_local"], "y": r["valeur_ugm3"],
                    "validated": r["validated"], "id_mesure": r["id_mesure"], "count": r["count"],
                })
            content = json.dumps(payload).encode()
            source = Path(raw_file).parts[1]                             # raw/<source>/date=…/fichier
            collected_at = str(g["_collected_at_utc"].iloc[0]).replace("+00:00", "Z")
            res = ingest(db, source, collected_at, raw_file, content)
            log.info("%s -> bronze #%s, %s lignes silver", raw_file, res.get("bronze_id"), res.get("lignes_silver"))
        except Exception as exc:
            errors += 1
            log.error("%s : %s", raw_file, exc)
    return errors + load_logs(db, history / "logs")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--history", type=Path, help="dossier créé par download_history.py (rattrapage)")
    a = p.parse_args()
    url, key = os.getenv("SUPABASE_URL", "").strip(), os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        log.info("SUPABASE_URL / SUPABASE_SERVICE_KEY absents : chargement Supabase ignoré")
        return
    db = Supabase(url, key)
    if a.history:
        errors = load_history(db, a.history)
    else:
        errors = load_raw(db) + load_logs(db, RAW_DIR / "_logs")
    if errors:
        log.warning("%d erreur(s) : les fichiers concernés seront retentés", errors)
    if not a.history:
        host = urlparse(url if "://" in url else f"https://{url}").netloc
        STATUS_FILE.write_text(json.dumps({
            "derniere_execution_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "architecture": "medaillon (bronze -> silver -> gold)",
            "projet": host.split(".")[0][:4] + "…" if host else None,      # jamais la clé
            "cle_type": "sb_secret" if key.startswith("sb_secret") else ("jwt" if key.startswith("eyJ") else "autre"),
            "erreurs": ERRORS,
        }, ensure_ascii=False, indent=1), encoding="utf-8")
    # Code de sortie 0 dans le workflow : l'archivage GitHub ne doit jamais être bloqué
    sys.exit(1 if errors and a.history else 0)


if __name__ == "__main__":
    main()
