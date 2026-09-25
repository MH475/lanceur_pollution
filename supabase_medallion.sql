-- =============================================================================
-- UrbanPulse – architecture médaillon dans Supabase (qualité de l'air Air Breizh)
-- =============================================================================
-- À exécuter UNE FOIS dans Supabase : SQL Editor -> New query -> coller -> Run.
-- Le script est ré-exécutable : il peut être relancé après une mise à jour.
--
--   BRONZE  bronze.airbreizh_raw     réponses brutes d'Air Breizh, telles quelles (jsonb),
--           bronze.ingestion_log     une ligne par fichier collecté ; jamais modifiées.
--                                    journal de chaque appel (statut, taille, erreur).
--   SILVER  silver.stations          référentiel des stations (type, coordonnées).
--           silver.mesures_air       une ligne par polluant × station × heure : typée,
--                                    dédoublonnée, contrôlée (colonne qualite).
--   GOLD    gold.*                   vues métier prêtes pour le dashboard et le modèle.
--
-- Flux : GitHub Actions -> public.ingest_airbreizh() -> bronze -> silver (SQL) -> gold.
-- Silver est entièrement reconstructible depuis bronze : select silver.rebuild();
-- =============================================================================

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

comment on schema bronze is 'Données brutes, immuables, telles que reçues des sources';
comment on schema silver is 'Données nettoyées, typées, dédoublonnées et contrôlées';
comment on schema gold   is 'Indicateurs métier prêts à l''usage (dashboard, modèle IA)';

-- -----------------------------------------------------------------------------
-- BRONZE
-- -----------------------------------------------------------------------------
create table if not exists bronze.airbreizh_raw (
    id            bigint generated always as identity primary key,
    source        text        not null,               -- airbreizh_no2, airbreizh_o3…
    polluant      text        not null,               -- NO2, O3, PM10, PM2.5, NH3
    collected_at  timestamptz not null,               -- heure de la collecte
    raw_file      text        not null unique,        -- chemin du fichier brut (GitHub)
    sha256        text,
    payload       jsonb       not null,               -- réponse Air Breizh complète
    ingested_at   timestamptz not null default now()
);
comment on table bronze.airbreizh_raw is
  'Réponses brutes Air Breizh (moyennes horaires 7 jours, stations de Rennes). Append-only. Licence ODbL.';

create table if not exists bronze.ingestion_log (
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

-- -----------------------------------------------------------------------------
-- SILVER
-- -----------------------------------------------------------------------------
create table if not exists silver.stations (
    station_code  text primary key,
    station       text not null,
    type_station  text not null,        -- urbaine trafic, urbaine fond, périurbaine fond
    commune       text not null,
    latitude      double precision,
    longitude     double precision,
    mise_en_service date
);

insert into silver.stations values
    ('HALLE',  'Halles',    'urbaine trafic',   'Rennes',    48.10753, -1.67975,  '1995-11-03'),
    ('LAENNE', 'Laënnec',   'urbaine trafic',   'Rennes',    48.108591, -1.665886, '1993-08-26'),
    ('THABOR', 'Thabor',    'urbaine fond',     'Rennes',    48.11521, -1.673088, '2022-12-15'),
    ('MORDEL', 'Mordelles', 'périurbaine fond', 'Mordelles', 48.0829,  -1.834569, '2018-11-20')
on conflict (station_code) do update set
    station = excluded.station, type_station = excluded.type_station, commune = excluded.commune,
    latitude = excluded.latitude, longitude = excluded.longitude, mise_en_service = excluded.mise_en_service;

create table if not exists silver.mesures_air (
    polluant       text             not null,
    station_code   text             not null references silver.stations (station_code),
    date_utc       timestamptz      not null,          -- début de l'heure mesurée
    date_locale    timestamp        not null,          -- même heure, heure de Paris
    valeur_ugm3    double precision,                   -- NULL = pas de mesure
    validated      numeric,                            -- statut Air Breizh (0 = provisoire)
    provisoire     boolean          not null,
    qualite        text             not null,          -- ok, manquant, negatif, aberrant
    id_mesure      text,
    collected_at   timestamptz      not null,          -- collecte qui a fourni la valeur
    bronze_id      bigint           not null references bronze.airbreizh_raw (id),
    primary key (polluant, station_code, date_utc)
);
create index if not exists mesures_air_date_idx on silver.mesures_air (date_utc);
comment on column silver.mesures_air.qualite is
  'ok | manquant (heure sans mesure) | negatif (valeur < 0) | aberrant (> 1000 µg/m³). Gold n''utilise que ok.';
comment on column silver.mesures_air.bronze_id is 'Lignage : fichier brut bronze d''où vient la valeur';

-- Bronze -> silver pour un fichier brut. Règles :
--  * une collecte plus récente remplace une plus ancienne (valeur revalidée par Air Breizh) ;
--  * une heure sans mesure n'écrase jamais une valeur existante ;
--  * l'ordre de traitement n'a pas d'importance (rejeu, rattrapage).
create or replace function silver.process_bronze(p_bronze_id bigint)
returns integer
language sql
set search_path = ''
as $$
    with points as (
        select r.id as bronze_id, r.polluant, r.collected_at,
               st.key as station_code, p
        from bronze.airbreizh_raw r
        cross join lateral jsonb_each(r.payload) as st(key, serie)
        cross join lateral jsonb_array_elements(st.serie) as p
        where r.id = p_bronze_id
          and jsonb_typeof(st.serie) = 'array'
          and p ? 'date_utc'
    ), typed as (
        select bronze_id, polluant, collected_at, station_code,
               ((p->>'date_utc')::timestamp at time zone 'UTC')                     as date_utc,
               coalesce((p->>'date_local')::timestamp,
                        ((p->>'date_utc')::timestamp at time zone 'UTC') at time zone 'Europe/Paris') as date_locale,
               round(nullif(p->>'y', '')::numeric, 2)::double precision             as valeur_ugm3,
               nullif(p->>'validated', '')::numeric                                 as validated,
               p->>'id_mesure'                                                      as id_mesure
        from points
    )
    insert into silver.mesures_air as m
        (polluant, station_code, date_utc, date_locale, valeur_ugm3, validated, provisoire,
         qualite, id_mesure, collected_at, bronze_id)
    select t.polluant, t.station_code, t.date_utc, t.date_locale, t.valeur_ugm3, t.validated,
           coalesce(t.validated, 0) = 0,
           case when t.valeur_ugm3 is null then 'manquant'
                when t.valeur_ugm3 < 0     then 'negatif'
                when t.valeur_ugm3 > 1000  then 'aberrant'
                else 'ok' end,
           t.id_mesure, t.collected_at, t.bronze_id
    from typed t
    join silver.stations s on s.station_code = t.station_code
    on conflict (polluant, station_code, date_utc) do update set
        date_locale  = excluded.date_locale,
        valeur_ugm3  = excluded.valeur_ugm3,
        validated    = excluded.validated,
        provisoire   = excluded.provisoire,
        qualite      = excluded.qualite,
        id_mesure    = excluded.id_mesure,
        collected_at = excluded.collected_at,
        bronze_id    = excluded.bronze_id
    where m.collected_at <= excluded.collected_at
      and (excluded.valeur_ugm3 is not null or m.valeur_ugm3 is null);
    select count(*)::integer from silver.mesures_air where bronze_id = p_bronze_id;
$$;

-- Reconstruit silver entièrement depuis bronze (après une correction de règle, par ex.)
create or replace function silver.rebuild()
returns integer
language plpgsql
set search_path = ''
as $$
declare
    r record;
begin
    delete from silver.mesures_air;
    for r in select id from bronze.airbreizh_raw order by collected_at, id loop
        perform silver.process_bronze(r.id);
    end loop;
    return (select count(*) from silver.mesures_air);
end;
$$;

-- -----------------------------------------------------------------------------
-- GOLD (vues : toujours à jour, aucun stockage en double – choix Green IT)
-- -----------------------------------------------------------------------------
-- Seuils de référence (µg/m³) : OMS 2021 en moyenne 24 h, UE (directive 2024/2881) en moyenne annuelle 2030
create or replace view gold.seuils as
select * from (values
    ('NO2',   25::numeric, 20::numeric),
    ('PM10',  45,          20),
    ('PM2.5', 15,          10),
    ('O3',    null,        null),
    ('NH3',   null,        null)
) as t(polluant, oms_24h, ue_annuel_2030);

-- NO2 horaire : stations trafic vs fond urbain ; écart = contribution locale du trafic
create or replace view gold.no2_ecart_trafic
with (security_invoker = true) as
select
    date_utc,
    min(date_locale) as date_locale,
    max(valeur_ugm3) filter (where station_code = 'HALLE')  as no2_halles,
    max(valeur_ugm3) filter (where station_code = 'LAENNE') as no2_laennec,
    max(valeur_ugm3) filter (where station_code = 'THABOR') as no2_thabor,
    max(valeur_ugm3) filter (where station_code = 'HALLE')
      - max(valeur_ugm3) filter (where station_code = 'THABOR') as ecart_trafic_halles_thabor,
    bool_or(provisoire) as contient_provisoire
from silver.mesures_air
where polluant = 'NO2' and qualite = 'ok'
group by date_utc;

-- Moyennes journalières (heure de Paris) et dépassements des seuils OMS
-- Règle : une journée n'est interprétée que si au moins 75 % des heures sont mesurées (18/24)
create or replace view gold.qualite_air_journaliere
with (security_invoker = true) as
select
    m.date_locale::date                                   as jour,
    m.polluant,
    m.station_code,
    s.station,
    s.type_station,
    round(avg(m.valeur_ugm3) filter (where m.qualite = 'ok')::numeric, 1) as moyenne_jour,
    round(max(m.valeur_ugm3) filter (where m.qualite = 'ok')::numeric, 1) as max_horaire,
    count(*) filter (where m.qualite = 'ok')              as heures_mesurees,
    round(count(*) filter (where m.qualite = 'ok') / 24.0, 2) as completude,
    count(*) filter (where m.qualite = 'ok') >= 18        as journee_valide,
    g.oms_24h,
    case when count(*) filter (where m.qualite = 'ok') >= 18 and g.oms_24h is not null
         then avg(m.valeur_ugm3) filter (where m.qualite = 'ok') > g.oms_24h end as depasse_oms_24h,
    bool_or(m.provisoire)                                 as contient_provisoire
from silver.mesures_air m
join silver.stations s using (station_code)
left join gold.seuils g using (polluant)
group by 1, 2, 3, 4, 5, g.oms_24h;

-- Dernière valeur valide par station et polluant (bandeau "maintenant" du dashboard)
create or replace view gold.dernieres_mesures
with (security_invoker = true) as
select distinct on (m.polluant, m.station_code)
    m.polluant, m.station_code, s.station, s.type_station, s.latitude, s.longitude,
    m.date_utc, m.date_locale, m.valeur_ugm3, m.provisoire,
    round(extract(epoch from now() - m.date_utc) / 3600)::integer as age_heures
from silver.mesures_air m
join silver.stations s using (station_code)
where m.qualite = 'ok'
order by m.polluant, m.station_code, m.date_utc desc;

-- Complétude par semaine : part des heures réellement mesurées (qualité des données, bloc 3)
create or replace view gold.completude_hebdo
with (security_invoker = true) as
select
    date_trunc('week', m.date_locale)::date                 as semaine,
    m.polluant, m.station_code, s.station,
    count(*)                                                as heures_presentes,
    count(*) filter (where m.qualite = 'ok')                as heures_valides,
    count(*) filter (where m.qualite = 'manquant')          as heures_manquantes,
    count(*) filter (where m.qualite in ('negatif', 'aberrant')) as heures_rejetees,
    round(count(*) filter (where m.qualite = 'ok') / 168.0, 3) as taux_completude
from silver.mesures_air m
join silver.stations s using (station_code)
group by 1, 2, 3, 4;

-- -----------------------------------------------------------------------------
-- POINT D'ENTRÉE pour GitHub Actions (seul objet exposé par l'API de Supabase)
-- -----------------------------------------------------------------------------
create or replace function public.ingest_airbreizh(
    p_source text, p_polluant text, p_collected_at timestamptz,
    p_raw_file text, p_sha256 text, p_payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_id bigint;
    v_new boolean := false;
    v_rows integer;
begin
    if jsonb_typeof(p_payload) <> 'object' then
        raise exception 'payload Air Breizh invalide (objet JSON attendu)';
    end if;
    insert into bronze.airbreizh_raw (source, polluant, collected_at, raw_file, sha256, payload)
    values (p_source, p_polluant, p_collected_at, p_raw_file, p_sha256, p_payload)
    on conflict (raw_file) do nothing
    returning id into v_id;
    if v_id is null then                              -- déjà en bronze : on rejoue silver
        select id into v_id from bronze.airbreizh_raw where raw_file = p_raw_file;
    else
        v_new := true;
    end if;
    v_rows := silver.process_bronze(v_id);
    return jsonb_build_object('bronze_id', v_id, 'nouveau', v_new, 'lignes_silver', v_rows);
end;
$$;

create or replace function public.ingest_log(p_rows jsonb)
returns integer
language sql
security definer
set search_path = ''
as $$
    insert into bronze.ingestion_log as l
    select * from jsonb_populate_recordset(null::bronze.ingestion_log, p_rows)
    on conflict (collected_at, source) do update set
        url = excluded.url, http_status = excluded.http_status, bytes = excluded.bytes,
        sha256 = excluded.sha256, n_records = excluded.n_records,
        source_timestamp = excluded.source_timestamp, stored = excluded.stored,
        duration_ms = excluded.duration_ms, error = excluded.error;
    select jsonb_array_length(p_rows);
$$;

-- -----------------------------------------------------------------------------
-- SÉCURITÉ
-- -----------------------------------------------------------------------------
-- Les schémas bronze/silver/gold ne sont PAS exposés par l'API REST : seules les
-- deux fonctions d'ingestion le sont, et uniquement pour la clé secrète (service_role).
alter table bronze.airbreizh_raw  enable row level security;
alter table bronze.ingestion_log  enable row level security;
alter table silver.stations       enable row level security;
alter table silver.mesures_air    enable row level security;

revoke all on schema bronze, silver, gold from public;
revoke all on all tables in schema bronze, silver, gold from public;
revoke execute on function public.ingest_airbreizh(text, text, timestamptz, text, text, jsonb) from public;
revoke execute on function public.ingest_log(jsonb) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        execute 'revoke all on schema bronze, silver, gold from anon, authenticated';
        execute 'revoke execute on function public.ingest_airbreizh(text, text, timestamptz, text, text, jsonb) from anon, authenticated';
        execute 'revoke execute on function public.ingest_log(jsonb) from anon, authenticated';
    end if;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant execute on function public.ingest_airbreizh(text, text, timestamptz, text, text, jsonb) to service_role';
        execute 'grant execute on function public.ingest_log(jsonb) to service_role';
        execute 'grant usage on schema bronze, silver, gold to service_role';
        execute 'grant select on all tables in schema bronze, silver, gold to service_role';
    end if;
end $$;

-- -----------------------------------------------------------------------------
-- MIGRATION depuis la première version (tables dans public)
-- -----------------------------------------------------------------------------
-- Le journal est conservé et déplacé en bronze ; l'ancienne table de mesures est
-- supprimée : silver est reconstruit depuis bronze au prochain chargement.
do $$
begin
    if to_regclass('public.ingestion_log') is not null then
        insert into bronze.ingestion_log select * from public.ingestion_log on conflict do nothing;
        drop table public.ingestion_log;
    end if;
end $$;
drop view  if exists public.v_no2_ecart_trafic;
drop table if exists public.mesures_air;
