create extension if not exists pg_trgm;

create table if not exists evi_bilanz_publications (
  id uuid primary key default gen_random_uuid(),
  evi_key text unique not null,

  publication_date date,
  publication_type text,
  detail_url text,
  source_item_path text,
  source_search_url text,

  company_name text,
  company_name_norm text,
  firmenbuchnummer text,
  search_text text,

  crawled_at timestamptz,
  imported_at timestamptz default now(),
  raw_row jsonb,

  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table evi_bilanz_publications add column if not exists evi_key text;
alter table evi_bilanz_publications add column if not exists publication_date date;
alter table evi_bilanz_publications add column if not exists publication_type text;
alter table evi_bilanz_publications add column if not exists detail_url text;
alter table evi_bilanz_publications add column if not exists source_item_path text;
alter table evi_bilanz_publications add column if not exists source_search_url text;
alter table evi_bilanz_publications add column if not exists company_name text;
alter table evi_bilanz_publications add column if not exists company_name_norm text;
alter table evi_bilanz_publications add column if not exists firmenbuchnummer text;
alter table evi_bilanz_publications add column if not exists search_text text;
alter table evi_bilanz_publications add column if not exists crawled_at timestamptz;
alter table evi_bilanz_publications add column if not exists imported_at timestamptz default now();
alter table evi_bilanz_publications add column if not exists raw_row jsonb;
alter table evi_bilanz_publications add column if not exists created_at timestamptz default now();
alter table evi_bilanz_publications add column if not exists updated_at timestamptz default now();

create unique index if not exists evi_bilanz_publications_evi_key_uq
on evi_bilanz_publications (evi_key);

create unique index if not exists evi_bilanz_publications_detail_url_uq
on evi_bilanz_publications (detail_url);

create index if not exists evi_bilanz_publications_date_idx
on evi_bilanz_publications (publication_date desc);

create index if not exists evi_bilanz_publications_fb_idx
on evi_bilanz_publications (firmenbuchnummer);

create index if not exists evi_bilanz_publications_company_trgm
on evi_bilanz_publications using gin (company_name gin_trgm_ops);

create index if not exists evi_bilanz_publications_search_text_trgm
on evi_bilanz_publications using gin (search_text gin_trgm_ops);
