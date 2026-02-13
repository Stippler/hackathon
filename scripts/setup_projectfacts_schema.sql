create extension if not exists pg_trgm;

create table if not exists projectfacts (
  id uuid primary key default gen_random_uuid(),
  pf_key text unique not null,

  name text,
  ort text,
  name_norm text,

  street text,
  plz text,
  city text,
  city_norm text,
  state text,
  country text,
  segment_country text,

  industries text,
  size text,

  last_changed_at timestamptz,
  last_activity_at timestamptz,

  company_address text,
  raw_addresses text,

  address_norm text,
  search_text text,
  raw_row jsonb,

  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table projectfacts add column if not exists pf_key text;
alter table projectfacts add column if not exists name text;
alter table projectfacts add column if not exists ort text;
alter table projectfacts add column if not exists name_norm text;
alter table projectfacts add column if not exists street text;
alter table projectfacts add column if not exists plz text;
alter table projectfacts add column if not exists city text;
alter table projectfacts add column if not exists city_norm text;
alter table projectfacts add column if not exists state text;
alter table projectfacts add column if not exists country text;
alter table projectfacts add column if not exists segment_country text;
alter table projectfacts add column if not exists industries text;
alter table projectfacts add column if not exists size text;
alter table projectfacts add column if not exists last_changed_at timestamptz;
alter table projectfacts add column if not exists last_activity_at timestamptz;
alter table projectfacts add column if not exists company_address text;
alter table projectfacts add column if not exists raw_addresses text;
alter table projectfacts add column if not exists address_norm text;
alter table projectfacts add column if not exists search_text text;
alter table projectfacts add column if not exists raw_row jsonb;
alter table projectfacts add column if not exists created_at timestamptz default now();
alter table projectfacts add column if not exists updated_at timestamptz default now();

create unique index if not exists projectfacts_pf_key_uq on projectfacts (pf_key);

create index if not exists pf_name_trgm
on projectfacts using gin (name gin_trgm_ops);

create index if not exists pf_address_trgm
on projectfacts using gin (company_address gin_trgm_ops);

create index if not exists pf_city_trgm
on projectfacts using gin (city gin_trgm_ops);

create index if not exists pf_search_text_trgm
on projectfacts using gin (search_text gin_trgm_ops);
