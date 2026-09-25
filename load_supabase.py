"""
UrbanPulse - chargement des mesures Air Breizh dans Supabase (PostgreSQL).

Lit les fichiers bruts collectés (data/raw/airbreizh_*/date=*/*.json.gz) qui n'ont
pas encore été chargés, et les envoie dans la table public.raw_mesures_air par l'API
REST de Supabase. Envoie aussi les journaux de collecte dans public.raw_ingestion_log.

- Clé de la table : (polluant, station_code, date_utc). Une heure déjà présente est
  mise à jour par une collecte plus récente (les semaines se chevauchent d'1 à 2 h,
  et Air Breizh valide les valeurs a posteriori).
- Une heure SANS mesure n'écrase jamais une valeur existante.
- Les fichiers chargés sont notés dans data/raw/_db_loaded.json : chaque fichier n'est
  envoyé qu'une fois ; en cas d'erreur, il est retenté à l'exécution suivante.
- Ne fait jamais échouer le workflow : une panne de Supabase ne doit pas empêcher
  l'archivage des données sur GitHub.

Variables d'environnement (secrets GitHub) :
    SUPABASE_URL          ex. https://abcdefgh.supabase.co
    SUPABASE_SERVICE_KEY  clé secrète (service_role ou sb_secret_...)

Usage :
    python load_supabase.py                        # fichiers bruts de data/raw
    python load_supabase.py --history history      # rattrapage depuis les Parquet téléchargés
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from urllib.parse import urlparse

import requests

from compact import parse_airbreizh

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"
STATE_FILE = RAW_DIR / "_db_loaded.json"      # redéfini par projet dans main()
STATUS_FILE = RAW_DIR / "_db_status.json"     # dernier résultat, lisible sur la branche data
ERRORS: list[str] = []
BATCH = 500
TIMEOUT_S = 60

# Noms des tables dans Supabase
TABLE_MESURES = "raw_mesures_air"
TABLE_JOURNAL = "raw_ingestion_log"

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

    def upsert(self, table: str, rows: list[dict], conflict: str, overwrite: bool = True) -> None:
        prefer = "resolution=merge-duplicates" if overwrite else "resolution=ignore-duplicates"
        for i in range(0, len(rows), BATCH):
            r = self.session.post(
                f"{self.base}/{table}", params={"on_conflict": conflict},
                headers={"Prefer": f"{prefer},return=minimal"},
                data=json.dumps(rows[i:i + BATCH]), timeout=TIMEOUT_S,
            )
            if r.status_code >= 300:
                raise RuntimeError(f"{table} : HTTP {r.status_code} {r.text[:300]}")


# --------------------------------------------------------------------------- conversion
def to_utc_iso(value: str | None) -> str | None:
    """'2026-09-18 12:00:00' (UTC chez Air Breizh) -> '2026-09-18T12:00:00Z'."""
    return value.replace(" ", "T") + "Z" if value else None


def to_float(value, ndigits: int | None = None):
    try:
        if value in (None, ""):
            return None
        x = float(value)
        return round(x, ndigits) if ndigits is not None else x
    except (TypeError, ValueError):
        return None


def mesure_rows(records: list[dict], polluant: str, collected_at: str, raw_file: str) -> list[dict]:
    rows = []
    for r in records:
        if not r.get("date_utc"):
            continue
        rows.append({
            "polluant": polluant,
            "station_code": r["station_code"],
            "station": r["station"],
            "date_utc": to_utc_iso(r["date_utc"]),
            "date_local": r["date_local"].replace(" ", "T") if r.get("date_local") else None,
            "valeur_ugm3": to_float(r.get("valeur_ugm3"), 2),     # source en float32 : 11.3999996 -> 11.4
            "validated": to_float(r.get("validated")),
            "id_mesure": r.get("id_mesure"),
            "collected_at": collected_at,
            "raw_file": raw_file,
        })
    return rows


def push_mesures(db: Supabase, rows: list[dict]) -> int:
    # Clé unique par lot (un même lot ne peut pas contenir deux fois la même heure)
    uniq = {(r["polluant"], r["station_code"], r["date_utc"]): r for r in rows}
    measured = [r for r in uniq.values() if r["valeur_ugm3"] is not None]
    missing = [r for r in uniq.values() if r["valeur_ugm3"] is None]
    conflict = "polluant,station_code,date_utc"
    db.upsert(TABLE_MESURES, measured, conflict, overwrite=True)
    db.upsert(TABLE_MESURES, missing, conflict, overwrite=False)   # trous : jamais d'écrasement
    return len(uniq)


def polluant_of(source: str) -> str:
    from sources import SOURCES
    return SOURCES[source]["polluant"]


# --------------------------------------------------------------------------- sources locales
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


def load_raw(db: Supabase) -> int:
    state = load_state()
    n_rows = errors = 0
    for path in sorted(RAW_DIR.glob("airbreizh_*/date=*/*.json.gz")):
        key = str(path.relative_to(RAW_DIR))
        if key in state:
            continue
        try:
            stamp = path.name.split("_")[-1].split(".")[0]              # 20260925T130933Z
            collected_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
            with gzip.open(path, "rb") as f:
                records = parse_airbreizh(f.read())
            rows = mesure_rows(records, polluant_of(path.parent.parent.name), collected_at, f"raw/{key}")
            n = push_mesures(db, rows)
            state[key] = datetime.now(timezone.utc).isoformat()
            n_rows += n
            log.info("%s : %d lignes envoyées", key, n)
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
        db.upsert(TABLE_JOURNAL, rows, "collected_at,source", overwrite=True)
        log.info("Journal : %d ligne(s) envoyée(s)", len(rows))
        return 0
    except Exception as exc:
        log.error("Journal non envoyé : %s", exc)
        ERRORS.append(f"ingestion_log : {exc}")
        return 1


def load_history(db: Supabase, history: Path) -> int:
    import pandas as pd
    errors = 0
    for part in sorted(history.glob("bronze/airbreizh_*/date=*/part-0.parquet")):
        try:
            df = pd.read_parquet(part)
            records = df.where(df.notna(), None).to_dict("records")
            rows = []
            for r in records:
                rows += mesure_rows([r], r["polluant"], r["_collected_at_utc"].replace("+00:00", "Z"), r["_raw_file"])
            log.info("%s : %d lignes envoyées", part.relative_to(history), push_mesures(db, rows))
        except Exception as exc:
            errors += 1
            log.error("%s : %s", part, exc)
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
    if not url.startswith("https://") or ".supabase.co" not in url:
        log.warning("SUPABASE_URL inhabituelle : attendu https://<projet>.supabase.co")
    db = Supabase(url, key)
    # Un fichier d'état par projet Supabase : en changeant de projet (secrets mis à jour),
    # toutes les données encore présentes sont renvoyées vers la nouvelle base.
    global STATE_FILE
    project = urlparse(url if "://" in url else f"https://{url}").netloc.split(".")[0] or "defaut"
    STATE_FILE = RAW_DIR / f"_db_loaded_{project}.json"
    if a.history:
        errors = load_history(db, a.history)
    else:
        errors = load_raw(db) + load_logs(db, RAW_DIR / "_logs")
    if errors:
        log.warning("%d erreur(s) : les fichiers concernés seront retentés", errors)
    if not a.history:
        host = urlparse(url).netloc
        STATUS_FILE.write_text(json.dumps({
            "derniere_execution_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "projet": host.split(".")[0][:4] + "…" if host else None,      # jamais la clé
            "cle_type": "sb_secret" if key.startswith("sb_secret") else ("jwt" if key.startswith("eyJ") else "autre"),
            "erreurs": ERRORS,
        }, ensure_ascii=False, indent=1), encoding="utf-8")
    # Code de sortie 0 dans le workflow : l'archivage GitHub ne doit jamais être bloqué
    sys.exit(1 if errors and a.history else 0)


if __name__ == "__main__":
    main()
