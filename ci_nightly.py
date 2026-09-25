"""
UrbanPulse - traitement de nuit pour GitHub Actions.

Pour chaque jour terminé (antérieur à aujourd'hui, UTC) encore présent dans data/raw :
  1. compaction en Parquet (compact.py) ;
  2. publication dans la Release GitHub du mois (tag data-AAAA-MM) :
       <source>__AAAA-MM-JJ.parquet      données bronze de la journée
       ingestion_log__AAAA-MM-JJ.csv     journal des appels de la journée
       <source>__<fichier brut>          sources non compactables (ex. GTFS zip)
  3. suppression des bruts du jour, une fois TOUT publié avec succès.

Écrit data/.squash si au moins un jour a été traité : le workflow repart alors
d'un historique vide sur la branche data, pour que le dépôt ne grossisse pas.

Usage : python ci_nightly.py [--dry-run]   (--dry-run : pas d'appel à GitHub)
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import compact
from collect import LOG_DIR, log_file_for
from sources import SOURCES

DATA_DIR = compact.DATA_DIR
RAW_DIR = compact.RAW_DIR
BRONZE_DIR = compact.BRONZE_DIR
SQUASH_FLAG = DATA_DIR / ".squash"

log = logging.getLogger("nightly")


def pending_days(today: date) -> list[date]:
    days = {date.fromisoformat(p.name.split("=")[1]) for p in RAW_DIR.glob("*/date=*")}
    days |= {date.fromisoformat(p.stem.rsplit("_", 1)[1]) for p in LOG_DIR.glob("ingestion_log_*.csv")}
    return sorted(d for d in days if d < today)


def gh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True)


def ensure_release(tag: str) -> None:
    if gh("release", "view", tag).returncode != 0:
        res = gh("release", "create", tag, "--title", f"Historique UrbanPulse {tag[5:]}",
                 "--notes", "Données collectées automatiquement (bronze Parquet + journaux). "
                            "Sources : Rennes Métropole, STAR (licence ODbL).")
        if res.returncode != 0 and "already exists" not in res.stderr:
            raise RuntimeError(f"Création de la release {tag} impossible : {res.stderr}")


def upload(tag: str, files: list[Path]) -> None:
    for i in range(0, len(files), 20):
        res = gh("release", "upload", tag, "--clobber", *map(str, files[i:i + 20]))
        if res.returncode != 0:
            raise RuntimeError(f"Upload vers {tag} échoué : {res.stderr}")


def process_day(day: date, dry_run: bool) -> bool:
    tag = f"data-{day:%Y-%m}"
    staging = Path(tempfile.mkdtemp(prefix=f"up_{day}_"))
    try:
        for name, cfg in SOURCES.items():             # toutes : publie aussi un reste éventuel
            compact.compact_source(name, day)
            raw_day = RAW_DIR / name / f"date={day:%Y-%m-%d}"
            if cfg["kind"] not in compact.PARSERS and raw_day.exists():
                for f in raw_day.glob("*.gz"):          # ex. GTFS zip : publié tel quel
                    shutil.copy(f, staging / f"{name}__{f.name}")
        for part in BRONZE_DIR.glob(f"*/date={day:%Y-%m-%d}/part-0.parquet"):
            source = part.parent.parent.name
            shutil.copy(part, staging / f"{source}__{day:%Y-%m-%d}.parquet")
        log_path = log_file_for(f"{day:%Y-%m-%d}")
        if log_path.exists():
            shutil.copy(log_path, staging / f"ingestion_log__{day:%Y-%m-%d}.csv")

        files = sorted(staging.iterdir())
        log.info("%s : %d fichier(s) à publier dans %s", day, len(files), tag)
        if files and not dry_run:
            ensure_release(tag)
            upload(tag, files)
    except Exception as exc:
        log.error("%s : publication interrompue, bruts conservés (%s)", day, exc)
        return False
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    # Tout est publié : nettoyage de la journée
    for d in RAW_DIR.glob(f"*/date={day:%Y-%m-%d}"):
        shutil.rmtree(d)
    for d in BRONZE_DIR.glob(f"*/date={day:%Y-%m-%d}"):
        shutil.rmtree(d)
    log_file_for(f"{day:%Y-%m-%d}").unlink(missing_ok=True)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    today = datetime.now(timezone.utc).date()
    days = pending_days(today)
    if not days:
        log.info("Rien à publier")
        return
    done = [d for d in days if process_day(d, a.dry_run)]
    shutil.rmtree(BRONZE_DIR, ignore_errors=True)
    if done:
        SQUASH_FLAG.write_text(",".join(map(str, done)), encoding="utf-8")


if __name__ == "__main__":
    main()
