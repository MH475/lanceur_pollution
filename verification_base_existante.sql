-- UrbanPulse – vérification AVANT d'installer le médaillon dans une base existante.
-- SQL Editor -> New query -> coller -> Run. Ce script ne modifie rien (lecture seule).
-- Si une ligne apparaît, l'objet existe déjà : envoyer le résultat avant d'aller plus loin.
select 'schéma'  as type_objet, nspname as nom, null::text as detail
from pg_namespace
where nspname in ('bronze', 'silver', 'gold')
union all
select case c.relkind when 'r' then 'table' when 'v' then 'vue' when 'm' then 'vue matérialisée' else c.relkind::text end,
       n.nspname || '.' || c.relname,
       coalesce(obj_description(c.oid, 'pg_class'), '(sans commentaire)')
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where (n.nspname in ('bronze', 'silver', 'gold'))
   or (n.nspname = 'public' and c.relname in ('mesures_air', 'ingestion_log', 'v_no2_ecart_trafic'))
union all
select 'fonction', n.nspname || '.' || p.proname, null
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public' and p.proname in ('ingest_airbreizh', 'ingest_log')
order by 1, 2;
