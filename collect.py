"""
UrbanPulse - collecteur d'historique (couche raw / landing).

Les sources temps réel de Rennes Métropole et du STAR ne publient que l'état
"maintenant" (moins de 24 h d'historique). Ce script les interroge à intervalle
régulier et archive chaque réponse brute, telle quelle, compressée :

    data/raw/<source>/date=AAAA-MM-JJ/<source>_<horodatage UTC>.<ext>.gz

Chaque appel est tracé dans data/raw/_logs/ingestion_log_<jour>.csv (horodatage, statut HTTP,
taille, empreinte SHA-256, nombre d'enregistrements, fraîcheur de la donnée
source, erreur éventuelle). Une réponse identique à la précédente n'est pas
réécrite (sobriété de stockage), mais l'appel reste journalisé.

Usage :
    python collect.py trafic              # collecte une source une fois
    python collect.py --all               # collecte toutes les sources une fois
    python collect.py --loop              # tourne en continu selon les intervalles
    python collect.py --due               # collecte ce qui est dû (GitHub Actions, cron)
    python collect.py --list              # liste les sources configurées
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from sources import ACTIVE, SOURCES

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
LOG_DIR = RAW_DIR / "_logs"            # un journal par jour UTC : ingestion_log_AAAA-MM-JJ.csv
RUN_STATE = RAW_DIR / "_last_run.json"  # dernière tentative par source (mode --due)
LOG_FIELDS = [
    "collected_at_utc", "source", "url", "http_status", "bytes", "sha256",
    "n_records", "source_timestamp", "stored", "duration_ms", "error",
]
HEADERS = {"User-Agent": "UrbanPulse-student-project/1.0 (M2 Data IA)"}
TIMEOUT_S = 60
RETRIES = 3

log = logging.getLogger("collect")


# ---------------------------------------------------------------------------
# Lecture rapide du contenu, pour le journal (nombre d'enregistrements, fraîcheur)
# ---------------------------------------------------------------------------
def inspect_payload(kind: str, content: bytes, cfg: dict) -> tuple[int | None, str | None]:
    """Retourne (nombre d'enregistrements, horodatage le plus récent de la source)."""
    try:
        if kind == "ods_json":
            records = json.loads(content)
            field = cfg.get("freshness_field")
            latest = max((r.get(field) for r in records if r.get(field)), default=None) if field else None
            return len(records), latest
        if kind == "geojson":
            return len(json.loads(content).get("features", [])), None
        if kind == "gbfs":
            doc = json.loads(content)
            ts = doc.get("last_updated")
            latest = datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None
            return len(doc.get("data", {}).get("stations", [])), latest
        if kind == "airbreizh":
            doc = json.loads(content)                 # {"HALLE": [{date_utc, y, validated…}, …], …}
            points = [p for serie in doc.values() for p in serie if p.get("y") is not None]
            latest = max((p["date_utc"] for p in points), default=None)
            return len(points), latest
        if kind == "gtfs_rt":
            from google.transit import gtfs_realtime_pb2
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(content)
            ts = feed.header.timestamp
            latest = datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None
            return len(feed.entity), latest
    except Exception as exc:  # le journal ne doit jamais bloquer l'archivage
        log.warning("Inspection impossible (%s) : %s", kind, exc)
    return None, None


# ---------------------------------------------------------------------------
# Résolution d'URL (GBFS : on passe par le fichier de découverte gbfs.json)
# ---------------------------------------------------------------------------
def resolve_url(cfg: dict, session: requests.Session) -> str:
    if "gbfs_feed" not in cfg:
        return cfg["url"]
    disco = session.get(cfg["url"], headers=HEADERS, timeout=TIMEOUT_S)
    disco.raise_for_status()
    data = disco.json()["data"]
    langs = list(data.values())
    for lang in langs:
        for feed in lang.get("feeds", []):
            if feed.get("name") == cfg["gbfs_feed"]:
                return feed["url"]
    raise ValueError(f"Flux GBFS '{cfg['gbfs_feed']}' absent de {cfg['url']}")


def fetch(url: str, session: requests.Session, post_data: list | None = None) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            if post_data is not None:
                resp = session.post(url, data=post_data, headers=HEADERS, timeout=TIMEOUT_S)
            else:
                resp = session.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            if resp.status_code < 500:
                return resp
            last_exc = RuntimeError(f"HTTP {resp.status_code}")
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(2 ** attempt)
    raise RuntimeError(f"Échec après {RETRIES} tentatives : {last_exc}")


# ---------------------------------------------------------------------------
# Collecte d'une source
# ---------------------------------------------------------------------------
STATE_FILE = RAW_DIR / "_last_hash.json"


def load_last_hash() -> dict[str, str]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_last_hash(state: dict[str, str]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=1), encoding="utf-8")


def log_file_for(day: str) -> Path:
    return LOG_DIR / f"ingestion_log_{day}.csv"


def write_log(row: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = log_file_for(row["collected_at_utc"][:10])
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


def collect(name: str, session: requests.Session | None = None) -> dict:
    cfg = SOURCES[name]
    session = session or requests.Session()
    now = datetime.now(timezone.utc)
    t0 = time.monotonic()
    row = {k: "" for k in LOG_FIELDS}
    row.update(collected_at_utc=now.isoformat(timespec="seconds"), source=name, stored=False)
    try:
        url = resolve_url(cfg, session)
        row["url"] = url
        resp = fetch(url, session, cfg.get("post_data"))
        row["http_status"] = resp.status_code
        resp.raise_for_status()
        content = resp.content
        if cfg["kind"] == "airbreizh" and not content.lstrip().startswith(b"{"):
            raise ValueError(f"Réponse inattendue d'Air Breizh : {content[:120]!r}")
        digest = hashlib.sha256(content).hexdigest()
        row.update(bytes=len(content), sha256=digest)
        n, latest = inspect_payload(cfg["kind"], content, cfg)
        row.update(n_records=n if n is not None else "", source_timestamp=latest or "")

        last_hash = load_last_hash()
        # Dédoublonnage limité à la journée : au moins un instantané par jour et par source
        stamp = f"{now:%Y-%m-%d}:{digest}"
        if last_hash.get(name) == stamp:
            log.info("%s : contenu inchangé, non réécrit", name)
        else:
            day_dir = RAW_DIR / name / f"date={now:%Y-%m-%d}"
            day_dir.mkdir(parents=True, exist_ok=True)
            path = day_dir / f"{name}_{now:%Y%m%dT%H%M%SZ}.{cfg['ext']}.gz"
            with gzip.open(path, "wb") as f:
                f.write(content)
            last_hash[name] = stamp
            save_last_hash(last_hash)
            row["stored"] = True
            log.info("%s : %s enregistrements -> %s", name, n, path.relative_to(DATA_DIR))
    except Exception as exc:
        row["error"] = str(exc)[:500]
        log.error("%s : %s", name, exc)
    row["duration_ms"] = int((time.monotonic() - t0) * 1000)
    write_log(row)
    return row


def run_due() -> list[dict]:
    """Collecte les sources actives dont l'intervalle est écoulé depuis la dernière réussite.

    Prévu pour un planificateur externe peu précis (GitHub Actions, parfois en
    retard) : l'état est gardé dans data/raw/_last_run.json. Une collecte en
    échec n'est pas comptée : elle est retentée à l'exécution suivante.
    Marge : 90 s, ou 1 % de l'intervalle pour les sources lentes (1 h 40 pour une
    source hebdomadaire), pour qu'un léger retard du planificateur ne décale pas
    la collecte d'un cycle entier.
    """
    try:
        state = json.loads(RUN_STATE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}
    now = time.time()
    session = requests.Session()
    results = []
    for name, cfg in ACTIVE.items():
        tolerance_s = max(90, cfg["every_min"] * 60 * 0.01)
        if now - state.get(name, 0) >= cfg["every_min"] * 60 - tolerance_s:
            row = collect(name, session)
            results.append(row)
            if not row["error"]:
                state[name] = now
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    RUN_STATE.write_text(json.dumps(state, indent=1), encoding="utf-8")
    if not results:
        log.info("Aucune source à collecter pour l'instant")
    return results


def loop() -> None:
    """Planificateur minimal : chaque source est collectée selon son intervalle."""
    session = requests.Session()
    next_run = {name: 0.0 for name in ACTIVE}
    log.info("Boucle démarrée pour : %s", ", ".join(ACTIVE))
    while True:
        now = time.time()
        for name, cfg in ACTIVE.items():
            if now >= next_run[name]:
                collect(name, session)
                next_run[name] = now + cfg["every_min"] * 60
        time.sleep(15)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("sources", nargs="*", help="noms des sources à collecter")
    p.add_argument("--all", action="store_true", help="collecter toutes les sources actives une fois")
    p.add_argument("--loop", action="store_true", help="tourner en continu")
    p.add_argument("--due", action="store_true", help="collecter les sources dont l'intervalle est écoulé (planificateur externe)")
    p.add_argument("--list", action="store_true", help="lister les sources")
    a = p.parse_args()

    if a.list:
        for name, cfg in SOURCES.items():
            state = "actif " if name in ACTIVE else "désact."
            print(f"{state} {name:28s} toutes les {cfg['every_min']:>5} min  {cfg['description']}")
        return
    if a.loop:
        loop()
        return
    if a.due:
        results = run_due()
        # Échec seulement si TOUTES les sources collectées ont échoué (panne réseau)
        sys.exit(1 if results and all(r["error"] for r in results) else 0)
    names = list(ACTIVE) if a.all else a.sources
    if not names:
        p.error("indiquez une source, --all, --loop ou --list")
    unknown = [n for n in names if n not in SOURCES]
    if unknown:
        p.error(f"source(s) inconnue(s) : {', '.join(unknown)}")
    errors = [collect(n) for n in names]
    sys.exit(1 if any(r["error"] for r in errors) else 0)


if __name__ == "__main__":
    main()
