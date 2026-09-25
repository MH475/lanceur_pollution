"""
UrbanPulse - compaction quotidienne raw -> bronze (Parquet).

Regroupe tous les instantanés d'une journée en un fichier Parquet par source :

    data/bronze/<source>/date=AAAA-MM-JJ/part-0.parquet

Chaque ligne garde les champs de la source tels quels, plus :
    _collected_at_utc : heure de collecte de l'instantané (clé de l'historique)
    _raw_file         : fichier brut d'origine (traçabilité)

Aucun nettoyage ici : c'est la couche bronze. Le nettoyage, la déduplication
et les agrégats au pas de 15 min se font ensuite en silver.

Usage :
    python compact.py                    # compacte la veille (UTC)
    python compact.py 2026-09-25         # compacte un jour donné
    python compact.py --purge-raw 30     # + supprime les bruts de plus de 30 jours
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from sources import ACTIVE, SOURCES

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"

# Champs lourds et invariants retirés des instantanés fréquents : la géométrie
# des tronçons ne change pas, elle est conservée une fois par jour à part.
DROP_FIELDS = {"trafic": {"geo_shape", "geo_point_2d"}}

log = logging.getLogger("compact")


def collected_at(path: Path) -> str:
    stamp = path.name.split("_")[-1].split(".")[0]          # 20260925T084200Z
    return datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()


def to_json_str(value):
    return json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value


# --------------------------------------------------------------------------- parseurs
def parse_ods_json(content: bytes) -> list[dict]:
    return [{k: to_json_str(v) for k, v in r.items()} for r in json.loads(content)]


def parse_geojson(content: bytes) -> list[dict]:
    rows = []
    for feat in json.loads(content).get("features", []):
        row = {k: to_json_str(v) for k, v in (feat.get("properties") or {}).items()}
        row["geometry"] = json.dumps(feat.get("geometry"), ensure_ascii=False)
        rows.append(row)
    return rows


def parse_gbfs(content: bytes) -> list[dict]:
    doc = json.loads(content)
    feed_ts = doc.get("last_updated")
    return [{**{k: to_json_str(v) for k, v in s.items()}, "feed_last_updated": feed_ts}
            for s in doc.get("data", {}).get("stations", [])]


def parse_gtfs_rt(content: bytes) -> list[dict]:
    from google.transit import gtfs_realtime_pb2
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    feed_ts = feed.header.timestamp or None
    rows = []
    for ent in feed.entity:
        if ent.HasField("trip_update"):
            tu = ent.trip_update
            base = {
                "feed_timestamp": feed_ts, "entity_id": ent.id,
                "trip_id": tu.trip.trip_id, "route_id": tu.trip.route_id,
                "direction_id": tu.trip.direction_id if tu.trip.HasField("direction_id") else None,
                "start_date": tu.trip.start_date, "start_time": tu.trip.start_time,
                "vehicle_id": tu.vehicle.id or None,
            }
            for stu in tu.stop_time_update:
                rows.append({
                    **base,
                    "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    "stop_id": stu.stop_id or None,
                    "arrival_time": stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None,
                    "arrival_delay_s": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None,
                    "departure_time": stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None,
                    "departure_delay_s": stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None,
                    "schedule_relationship": stu.schedule_relationship,
                })
        elif ent.HasField("vehicle"):
            v = ent.vehicle
            rows.append({
                "feed_timestamp": feed_ts, "entity_id": ent.id,
                "trip_id": v.trip.trip_id, "route_id": v.trip.route_id,
                "vehicle_id": v.vehicle.id or None,
                "latitude": v.position.latitude if v.HasField("position") else None,
                "longitude": v.position.longitude if v.HasField("position") else None,
                "bearing": v.position.bearing if v.HasField("position") else None,
                "stop_id": v.stop_id or None,
                "current_status": v.current_status if v.HasField("current_status") else None,
                "vehicle_timestamp": v.timestamp or None,
            })
    return rows


def parse_airbreizh(content: bytes) -> list[dict]:
    """{"HALLE": [{"date_utc", "date_local", "y", "validated", "id_mesure", …}, …], …}
    -> une ligne par station et par heure. Les heures sans mesure (y = null) sont
    gardées : elles signalent un trou de mesure côté Air Breizh."""
    from sources import AIRBREIZH_STATIONS
    rows = []
    for code, serie in json.loads(content).items():
        for p in serie:
            y = p.get("y")
            rows.append({
                "station_code": code,
                "station": AIRBREIZH_STATIONS.get(code, code),
                "id_mesure": p.get("id_mesure"),
                "date_utc": p.get("date_utc"),
                "date_local": p.get("date_local"),
                "valeur_ugm3": float(y) if y is not None else None,
                # Statut de validation fourni par Air Breizh ("0.0000" sur les heures récentes,
                # non encore validées) : à conserver, les valeurs peuvent être corrigées ensuite
                "validated": p.get("validated"),
                "unvalidated": p.get("unvalidated"),
                "count": p.get("count"),
            })
    return rows


PARSERS = {"airbreizh": parse_airbreizh, "ods_json": parse_ods_json, "geojson": parse_geojson, "gbfs": parse_gbfs, "gtfs_rt": parse_gtfs_rt}


# --------------------------------------------------------------------------- compaction
def compact_source(name: str, day: date) -> int:
    cfg = SOURCES[name]
    parser = PARSERS.get(cfg["kind"])
    day_dir = RAW_DIR / name / f"date={day:%Y-%m-%d}"
    if parser is None or not day_dir.exists():
        return 0
    frames, errors = [], 0
    drop = DROP_FIELDS.get(name, set())
    reference_kept = False
    for path in sorted(day_dir.glob("*.gz")):
        try:
            with gzip.open(path, "rb") as f:
                rows = parser(f.read())
        except Exception as exc:
            errors += 1
            log.warning("%s illisible : %s", path.name, exc)
            continue
        if not rows:
            continue
        df = pd.DataFrame(rows)
        if drop and not reference_kept:
            # Premier instantané du jour conservé en entier (avec géométrie) comme référence
            out = BRONZE_DIR / f"{name}_geometrie" / f"date={day:%Y-%m-%d}"
            out.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out / "part-0.parquet", index=False)
            reference_kept = True
        df = df.drop(columns=[c for c in drop if c in df.columns])
        if cfg.get("polluant"):
            df.insert(0, "polluant", cfg["polluant"])
        df["_collected_at_utc"] = collected_at(path)
        df["_raw_file"] = str(path.relative_to(DATA_DIR))
        frames.append(df)
    if not frames:
        return 0
    out_dir = BRONZE_DIR / name / f"date={day:%Y-%m-%d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    full = pd.concat(frames, ignore_index=True)
    # Colonnes de types mêlés -> texte, pour un schéma Parquet stable
    for col in full.columns:
        if full[col].dtype == object:
            full[col] = full[col].map(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
    full.to_parquet(out_dir / "part-0.parquet", index=False)
    log.info("%s %s : %d instantanés, %d lignes, %d fichier(s) illisible(s)",
             name, day, len(frames), len(full), errors)
    return len(full)


def purge_raw(older_than_days: int) -> None:
    limit = datetime.now(timezone.utc).date() - timedelta(days=older_than_days)
    for day_dir in RAW_DIR.glob("*/date=*"):
        d = date.fromisoformat(day_dir.name.split("=")[1])
        compacted = (BRONZE_DIR / day_dir.parent.name / day_dir.name / "part-0.parquet").exists()
        # Les bruts non compactables (ex. GTFS zip) sont la seule copie : jamais purgés
        if d < limit and compacted:
            shutil.rmtree(day_dir)
            log.info("Brut supprimé : %s", day_dir.relative_to(DATA_DIR))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("day", nargs="?", help="jour à compacter (AAAA-MM-JJ), défaut : la veille")
    p.add_argument("--purge-raw", type=int, metavar="JOURS", help="supprimer les bruts compactés plus vieux que JOURS")
    a = p.parse_args()
    day = date.fromisoformat(a.day) if a.day else datetime.now(timezone.utc).date() - timedelta(days=1)
    for name in ACTIVE:
        compact_source(name, day)
    if a.purge_raw:
        purge_raw(a.purge_raw)


if __name__ == "__main__":
    main()
