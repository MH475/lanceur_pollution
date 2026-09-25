"""
Sources à historiser et fréquence de collecte.

Configuration actuelle : SEULE la qualité de l'air Air Breizh est active
(mesures horaires des 7 derniers jours, un fichier par gaz, une fois par semaine).
Les autres sources sont conservées mais désactivées ("enabled": False) :
passer "enabled" à True pour les réactiver.

Attention : sans la source "trafic", aucun historique de trafic n'est constitué,
or c'est la cible du modèle de prévision de congestion et Rennes Métropole n'en
publie pas.

Règle de choix de la fréquence : pas plus souvent que nécessaire (Green IT).
Air Breizh publie 169 valeurs horaires (7 jours + 1 h) par appel : une collecte
hebdomadaire couvre la semaine entière, avec 1 à 2 h de recouvrement.
"""

AIRBREIZH_URL = "https://www.airbreizh.asso.fr/wp-admin/admin-ajax.php"
# Stations de l'agglomération rennaise (codes du formulaire de la page Rennes)
AIRBREIZH_STATIONS = {"HALLE": "Halles", "LAENNE": "Laënnec", "MORDEL": "Mordelles", "THABOR": "Thabor"}
# Gaz disponibles et stations qui les mesurent (vérifié le 25/09/2026)
AIRBREIZH_POLLUANTS = {
    "NH3": ["MORDEL"],
    "NO2": ["HALLE", "LAENNE", "THABOR"],
    "O3": ["MORDEL", "THABOR"],
    "PM10": ["LAENNE", "THABOR"],
    "PM2.5": ["LAENNE", "THABOR"],
}

ODS_RM = "https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets"
GTFS_RT = "https://proxy.transport.data.gouv.fr/resource"

SOURCES: dict[str, dict] = {
    # --- Qualité de l'air Air Breizh : 1 fichier par gaz, 1 fois par semaine ----
    "airbreizh_nh3": {
        "description": "Air Breizh NH3, moyennes horaires des 7 derniers jours (Rennes)",
        "url": AIRBREIZH_URL,
        "post_data": [("action", "getDatasForGraph"), ("polluant", "NH3"), ("periode", "week"), ("type", "")]
        + [("stations[]", s) for s in AIRBREIZH_POLLUANTS["NH3"]],
        "kind": "airbreizh",
        "ext": "json",
        "polluant": "NH3",
        "enabled": True,
        "every_min": 7 * 24 * 60,
    },
    "airbreizh_no2": {
        "description": "Air Breizh NO2, moyennes horaires des 7 derniers jours (Rennes)",
        "url": AIRBREIZH_URL,
        "post_data": [("action", "getDatasForGraph"), ("polluant", "NO2"), ("periode", "week"), ("type", "")]
        + [("stations[]", s) for s in AIRBREIZH_POLLUANTS["NO2"]],
        "kind": "airbreizh",
        "ext": "json",
        "polluant": "NO2",
        "enabled": True,
        "every_min": 7 * 24 * 60,
    },
    "airbreizh_o3": {
        "description": "Air Breizh O3, moyennes horaires des 7 derniers jours (Rennes)",
        "url": AIRBREIZH_URL,
        "post_data": [("action", "getDatasForGraph"), ("polluant", "O3"), ("periode", "week"), ("type", "")]
        + [("stations[]", s) for s in AIRBREIZH_POLLUANTS["O3"]],
        "kind": "airbreizh",
        "ext": "json",
        "polluant": "O3",
        "enabled": True,
        "every_min": 7 * 24 * 60,
    },
    "airbreizh_pm10": {
        "description": "Air Breizh PM10, moyennes horaires des 7 derniers jours (Rennes)",
        "url": AIRBREIZH_URL,
        "post_data": [("action", "getDatasForGraph"), ("polluant", "PM10"), ("periode", "week"), ("type", "")]
        + [("stations[]", s) for s in AIRBREIZH_POLLUANTS["PM10"]],
        "kind": "airbreizh",
        "ext": "json",
        "polluant": "PM10",
        "enabled": True,
        "every_min": 7 * 24 * 60,
    },
    "airbreizh_pm25": {
        "description": "Air Breizh PM2.5, moyennes horaires des 7 derniers jours (Rennes)",
        "url": AIRBREIZH_URL,
        "post_data": [("action", "getDatasForGraph"), ("polluant", "PM2.5"), ("periode", "week"), ("type", "")]
        + [("stations[]", s) for s in AIRBREIZH_POLLUANTS["PM2.5"]],
        "kind": "airbreizh",
        "ext": "json",
        "polluant": "PM2.5",
        "enabled": True,
        "every_min": 7 * 24 * 60,
    },
    # --- Trafic : cible du modèle de prévision de congestion -----------------
    "trafic": {
        "description": "État du trafic temps réel (FCD, ~2 900 tronçons, maj 3 min)",
        "url": f"{ODS_RM}/etat-du-trafic-en-temps-reel/exports/json",
        "kind": "ods_json",
        "ext": "json",
        "enabled": False,
        "every_min": 5,              # source à 3 min ; 5 min garde les pics, 12 points/h
        "freshness_field": "datetime",
    },
    # --- Chantiers : feature "chantier actif" + contrôle de fraîcheur (bloc 3)
    "travaux_1_jour": {
        "description": "Travaux de voirie du jour courant",
        "url": f"{ODS_RM}/travaux_1_jour/exports/geojson",
        "kind": "geojson",
        "ext": "geojson",
        "enabled": False,
        "every_min": 60,
    },
    "travaux_6_jours": {
        "description": "Travaux de voirie des 6 prochains jours",
        "url": f"{ODS_RM}/travaux_6_jours/exports/geojson",
        "kind": "geojson",
        "ext": "geojson",
        "enabled": False,
        "every_min": 360,
    },
    # --- Transport public STAR -------------------------------------------------
    "star_trip_updates": {
        "description": "STAR GTFS-RT, retards prévus par course et par arrêt",
        "url": f"{GTFS_RT}/star-rennes-integration-gtfs-rt-trip-update",
        "kind": "gtfs_rt",
        "ext": "pb",
        "enabled": False,
        "every_min": 5,
    },
    "star_vehicle_positions": {
        "description": "STAR GTFS-RT, positions des véhicules",
        "url": f"{GTFS_RT}/star-rennes-integration-gtfs-rt-vehicle-position",
        "kind": "gtfs_rt",
        "ext": "pb",
        "enabled": False,
        "every_min": 5,
    },
    "star_gtfs": {
        "description": "STAR GTFS théorique en vigueur (horaires prévus)",
        "url": "https://eu.ftp.opendatasoft.com/star/gtfs/GTFS_STAR_BUS_METRO_EN_COURS.zip",
        "kind": "binary",
        "ext": "zip",
        "enabled": False,
        "every_min": 1440,
    },
    # --- Vélo en libre-service STAR (GBFS) -------------------------------------
    "vls_station_status": {
        "description": "Vélos STAR : disponibilité par station",
        "url": "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json",
        "gbfs_feed": "station_status",
        "kind": "gbfs",
        "ext": "json",
        "enabled": False,
        "every_min": 10,
    },
    "vls_station_information": {
        "description": "Vélos STAR : référentiel des stations",
        "url": "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json",
        "gbfs_feed": "station_information",
        "kind": "gbfs",
        "ext": "json",
        "enabled": False,
        "every_min": 1440,
    },
}

# Sources réellement collectées
ACTIVE: dict[str, dict] = {name: cfg for name, cfg in SOURCES.items() if cfg.get("enabled", True)}
