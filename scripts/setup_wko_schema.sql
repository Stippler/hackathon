create extension if not exists pg_trgm;

create table if not exists wko_branches (
  id uuid primary key default gen_random_uuid(),
  branche text not null,
  branch_url text not null,
  letter text,
  source text,
  discovered_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (branche, branch_url)
);

create table if not exists wko_companies (
  id uuid primary key default gen_random_uuid(),
  wko_key text unique not null,

  branche text,
  name text,
  wko_detail_url text,
  company_website text,
  email text,
  phone text,
  street text,
  zip_city text,
  address text,
  source_list_url text,

  crawled_at timestamptz,
  imported_at timestamptz default now(),

  search_text text,
  raw_row jsonb,

  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table wko_branches add column if not exists branche text;
alter table wko_branches add column if not exists branch_url text;
alter table wko_branches add column if not exists letter text;
alter table wko_branches add column if not exists source text;
alter table wko_branches add column if not exists discovered_at timestamptz;
alter table wko_branches add column if not exists created_at timestamptz default now();
alter table wko_branches add column if not exists updated_at timestamptz default now();

alter table wko_companies add column if not exists wko_key text;
alter table wko_companies add column if not exists branche text;
alter table wko_companies add column if not exists name text;
alter table wko_companies add column if not exists wko_detail_url text;
alter table wko_companies add column if not exists company_website text;
alter table wko_companies add column if not exists email text;
alter table wko_companies add column if not exists phone text;
alter table wko_companies add column if not exists street text;
alter table wko_companies add column if not exists zip_city text;
alter table wko_companies add column if not exists address text;
alter table wko_companies add column if not exists source_list_url text;
alter table wko_companies add column if not exists crawled_at timestamptz;
alter table wko_companies add column if not exists imported_at timestamptz default now();
alter table wko_companies add column if not exists search_text text;
alter table wko_companies add column if not exists raw_row jsonb;
alter table wko_companies add column if not exists created_at timestamptz default now();
alter table wko_companies add column if not exists updated_at timestamptz default now();

create unique index if not exists wko_companies_wko_key_uq on wko_companies (wko_key);
create unique index if not exists wko_branches_branche_url_uq on wko_branches (branche, branch_url);

create index if not exists wko_companies_detail_url_idx on wko_companies (wko_detail_url);
create index if not exists wko_companies_branche_idx on wko_companies (branche);
create index if not exists wko_companies_crawled_at_idx on wko_companies (crawled_at);
create index if not exists wko_branches_letter_idx on wko_branches (letter);

create index if not exists wko_name_trgm on wko_companies using gin (name gin_trgm_ops);
create index if not exists wko_address_trgm on wko_companies using gin (address gin_trgm_ops);
create index if not exists wko_search_text_trgm on wko_companies using gin (search_text gin_trgm_ops);

