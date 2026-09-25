-- UrbanPulse – tables Supabase pour la qualité de l'air Air Breizh (Rennes)
-- À exécuter UNE FOIS dans Supabase : SQL Editor -> New query -> coller -> Run.

-- 1. Mesures horaires : une ligne par gaz, par station et par heure ------------
create table if not exists public.raw_mesures_air (
    polluant        text             not null,            -- NO2, O3, PM10, PM2.5, NH3
    station_code    text             not null,            -- HALLE, LAENNE, MORDEL, THABOR
    station         text             not null,            -- Halles, Laënnec, Mordelles, Thabor
    date_utc        timestamptz      not null,            -- début de l'heure mesurée (UTC)
    date_local      timestamp,                            -- même heure, heure de Paris
    valeur_ugm3     double precision,                     -- moyenne horaire ; NULL = pas de mesure
    validated       numeric,                              -- statut de validation Air Breizh (0 = provisoire)
    id_mesure       text,                                 -- identifiant de la série chez Air Breizh
    collected_at    timestamptz      not null,            -- heure de la collecte qui a fourni la valeur
    raw_file        text,                                 -- fichier brut d'origine (traçabilité)
    primary key (polluant, station_code, date_utc)
);

comment on table public.raw_mesures_air is
  'Moyennes horaires Air Breizh (stations de Rennes), collectées chaque semaine par GitHub Actions. Licence ODbL, source : Air Breizh.';

create index if not exists raw_mesures_air_date_idx on public.raw_mesures_air (date_utc);

-- 2. Journal des collectes (traçabilité, fraîcheur, erreurs) ---------------------
create table if not exists public.raw_ingestion_log (
    collected_at      timestamptz not null,
    source            text        not null,
    url               text,
    http_status       integer,
    bytes             integer,
    sha256            text,
    n_records         integer,
    source_timestamp  text,
    stored            boolean,
    duration_ms       integer,
    error             text,
    primary key (collected_at, source)
);

-- 3. Sécurité : Row Level Security activé, sans aucune règle d'accès public ------
-- Seule la clé secrète (service_role), gardée dans les secrets GitHub, peut écrire.
-- Les clés publiques (anon) ne peuvent ni lire ni écrire.
alter table public.raw_mesures_air   enable row level security;
alter table public.raw_ingestion_log enable row level security;

-- 4. Vue pratique : NO2 par heure, stations trafic vs fond urbain ----------------
-- Écart Halles - Thabor = estimation de la contribution locale du trafic.
create or replace view public.v_no2_ecart_trafic
with (security_invoker = true) as
select
    date_utc,
    max(valeur_ugm3) filter (where station_code = 'HALLE')  as no2_halles,
    max(valeur_ugm3) filter (where station_code = 'LAENNE') as no2_laennec,
    max(valeur_ugm3) filter (where station_code = 'THABOR') as no2_thabor,
    max(valeur_ugm3) filter (where station_code = 'HALLE')
      - max(valeur_ugm3) filter (where station_code = 'THABOR') as ecart_trafic_halles_thabor
from public.raw_mesures_air
where polluant = 'NO2'
group by date_utc
order by date_utc;
