"""
UrbanPulse - rapatrier l'historique publié dans les Releases GitHub.

Télécharge les fichiers <source>__AAAA-MM-JJ.parquet de toutes les releases
data-AAAA-MM du dépôt et les range comme une table partitionnée :

    history/bronze/<source>/date=AAAA-MM-JJ/part-0.parquet
    history/logs/ingestion_log__AAAA-MM-JJ.csv

Les fichiers déjà présents ne sont pas retéléchargés.

Usage :
    python download_history.py <proprietaire>/<depot>
    python download_history.py <proprietaire>/<depot> --source trafic --since 2026-10-01

Puis, pour l'entraînement :
    import pandas as pd
    trafic = pd.read_parquet("history/bronze/trafic")
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import requests

API = "https://api.github.com"


def list_assets(repo: str, session: requests.Session):
    url = f"{API}/repos/{repo}/releases?per_page=100"
    while url:
        r = session.get(url, timeout=60)
        r.raise_for_status()
        for rel in r.json():
            if rel["tag_name"].startswith("data-"):
                yield from rel["assets"]
        url = r.links.get("next", {}).get("url")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("repo", help="propriétaire/dépôt GitHub, ex. malo-hery/urbanpulse-collector")
    p.add_argument("--source", action="append", help="ne télécharger que cette source (répétable)")
    p.add_argument("--since", help="ne télécharger qu'à partir de cette date (AAAA-MM-JJ)")
    p.add_argument("--out", default="history", help="dossier de destination (défaut : history)")
    a = p.parse_args()

    session = requests.Session()
    session.headers["Accept"] = "application/vnd.github+json"
    if os.getenv("GITHUB_TOKEN"):                       # facultatif : relève la limite d'appels
        session.headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"

    out = Path(a.out)
    n_new = n_skip = 0
    for asset in list_assets(a.repo, session):
        name = asset["name"]
        if "__" not in name:
            continue
        source, rest = name.split("__", 1)
        if name.endswith(".parquet"):
            day = rest.removesuffix(".parquet")
            dest = out / "bronze" / source / f"date={day}" / "part-0.parquet"
        elif source == "ingestion_log":
            day = rest.removesuffix(".csv")
            dest = out / "logs" / name
        else:                                            # bruts non compactés (GTFS zip…)
            day = rest.rsplit("_", 1)[-1][:8]              # ..._20260925T125000Z.zip.gz
            day = f"{day[:4]}-{day[4:6]}-{day[6:8]}"
            dest = out / "raw" / source / rest
        if a.source and source not in a.source and source != "ingestion_log":
            continue
        if a.since and day < a.since:
            continue
        if dest.exists() and dest.stat().st_size == asset["size"]:
            n_skip += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        with session.get(asset["browser_download_url"], stream=True, timeout=300) as r:
            r.raise_for_status()
            with dest.open("wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
        n_new += 1
        print(f"téléchargé : {dest}")
    print(f"Terminé : {n_new} nouveau(x) fichier(s), {n_skip} déjà présent(s).")


if __name__ == "__main__":
    main()
