create extension if not exists pg_trgm;
create extension if not exists pgcrypto;

-- source-driven continuous crawl queue (WKO / EVI / Projectfacts)
create table if not exists ofb_crawl_queue (
  id uuid primary key default gen_random_uuid(),
  source_system text not null, -- wko | evi | projectfacts | manual
  source_key text not null,
  source_name text,
  source_context jsonb,
  search_name text not null,
  search_name_norm text,
  firmennummer text,
  status text not null default 'pending', -- pending | running | done | failed
  priority int not null default 100,
  attempts int not null default 0,
  next_run_at timestamptz not null default now(),
  last_error text,
  last_run_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (source_system, source_key)
);

-- canonical company row keyed by Firmenbuchnummer
create table if not exists ofb_companies (
  id uuid primary key default gen_random_uuid(),
  firmennummer text unique not null,
  court_code text,
  court_text text,
  final_status text,
  final_names text[],
  final_seat text,
  final_legal_form_text text,
  final_legal_form_code text,
  final_right_property text,
  euid text,
  first_seen_at timestamptz default now(),
  last_seen_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- search request/response log: /firmenbuch/suche/firma/compressed
create table if not exists ofb_search_runs (
  id uuid primary key default gen_random_uuid(),
  request_firmenwortlaut text not null,
  request_exaktesuche boolean not null,
  request_suchbereich int,
  request_gericht text,
  request_rechtsform text,
  request_rechtseigenschaft text,
  request_ortnr text,
  response_count int,
  raw_response jsonb,
  ran_at timestamptz default now()
);

create table if not exists ofb_search_results (
  id uuid primary key default gen_random_uuid(),
  search_run_id uuid not null references ofb_search_runs(id) on delete cascade,
  firmennummer text not null,
  court_text text,
  court_code text,
  final_status text,
  final_names text[],
  final_seat text,
  final_legal_form_text text,
  final_legal_form_code text,
  final_right_property text,
  raw_result jsonb,
  created_at timestamptz default now(),
  unique (search_run_id, firmennummer)
);

create table if not exists ofb_search_result_changes (
  id uuid primary key default gen_random_uuid(),
  search_result_id uuid not null references ofb_search_results(id) on delete cascade,
  seq_no int not null,
  names text[],
  legal_form_text text,
  legal_form_code text,
  seat text,
  raw_change jsonb,
  unique (search_result_id, seq_no)
);

-- /firmenbuch/auszug top-level snapshot
create table if not exists ofb_auszug_snapshots (
  id uuid primary key default gen_random_uuid(),
  firmennummer text not null,
  stichtag date not null,
  umfang text,
  pruefsumme text,
  abfragezeitpunkt timestamptz,
  metadaten jsonb,
  kur jsonb,
  ident jsonb,
  zwl jsonb,
  raw_response jsonb,
  created_at timestamptz default now()
);

create table if not exists ofb_auszug_vollz (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  vnr text not null,
  antragstext text[],
  vollzugsdatum date,
  hg_code text,
  hg_text text,
  eingelangt_am date,
  az text,
  raw_row jsonb,
  unique (snapshot_id, vnr)
);

create table if not exists ofb_auszug_euid (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  znr text,
  euid text,
  raw_row jsonb
);

create table if not exists ofb_auszug_fun (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  pnr text,
  fken text,
  fkentext text,
  rechtstatsache jsonb,
  fu_dkz11 jsonb,
  fu_dkz12 jsonb,
  raw_row jsonb
);

create table if not exists ofb_auszug_fun_dkz10 (
  id uuid primary key default gen_random_uuid(),
  fun_id uuid not null references ofb_auszug_fun(id) on delete cascade,
  seq_no int not null,
  vertretungsbefugtnurfuer jsonb,
  txtvertr text[],
  aufrecht boolean,
  datvon text, -- API returns YYYYMMDD format
  datbis text,
  vart_code text,
  vart_text text,
  vsbeide text,
  whr text,
  kapital numeric,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  bezugsperson text,
  text_lines text[],
  vnr text,
  raw_row jsonb,
  unique (fun_id, seq_no)
);

create table if not exists ofb_auszug_per (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  pnr text not null,
  pe_dkz03 jsonb,
  pe_dkz06 jsonb,
  pe_dkz09 jsonb,
  pe_staat jsonb,
  rechtstatsache jsonb,
  raw_row jsonb,
  unique (snapshot_id, pnr)
);

create table if not exists ofb_auszug_per_dkz02 (
  id uuid primary key default gen_random_uuid(),
  per_id uuid not null references ofb_auszug_per(id) on delete cascade,
  seq_no int not null,
  name_formatiert text[],
  aufrecht boolean,
  titelvor text,
  vorname text,
  nachname text,
  titelnach text,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  bezeichnung text[],
  geburtsdatum text, -- API returns YYYYMMDD format
  vnr text,
  raw_row jsonb,
  unique (per_id, seq_no)
);

create table if not exists ofb_auszug_firma_dkz02 (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  seq_no int not null,
  aufrecht boolean,
  ausland text,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  bezeichnung text[],
  vnr text,
  raw_row jsonb,
  unique (snapshot_id, seq_no)
);

create table if not exists ofb_auszug_firma_dkz03 (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  seq_no int not null,
  zustellbar boolean,
  aufrecht boolean,
  stelle text[],
  ort text,
  staat text,
  plz text,
  strasse text,
  stiege text,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  zustellanweisung text,
  hausnummer text,
  tuernummer text,
  vnr text,
  raw_row jsonb,
  unique (snapshot_id, seq_no)
);

create table if not exists ofb_auszug_firma_dkz06 (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  seq_no int not null,
  aufrecht boolean,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  ortnr_code text,
  ortnr_text text,
  sitz text,
  vnr text,
  raw_row jsonb,
  unique (snapshot_id, seq_no)
);

create table if not exists ofb_auszug_firma_dkz07 (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references ofb_auszug_snapshots(id) on delete cascade,
  seq_no int not null,
  aufrecht boolean,
  mit_firma_geloescht_durch_vnr text,
  mit_zwl_geloescht_durch_vnr text,
  vnr text,
  rechtsform_code text,
  rechtsform_text text,
  raw_row jsonb,
  unique (snapshot_id, seq_no)
);

-- /firmenbuch/urkunde/daten/multiple fiscal-year wrapper
create table if not exists ofb_financial_years (
  id uuid primary key default gen_random_uuid(),
  firmennummer text not null,
  gj_beginn timestamptz not null,
  gj_ende timestamptz not null,
  raw_row jsonb,
  created_at timestamptz default now(),
  unique (firmennummer, gj_beginn, gj_ende)
);

create table if not exists ofb_financial_bilanz (
  id uuid primary key default gen_random_uuid(),
  financial_year_id uuid not null references ofb_financial_years(id) on delete cascade,
  bilanz_summe numeric,
  bilanz_summe_vj numeric,
  anlage_vermoegen numeric,
  anlage_vermoegen_vj numeric,
  immaterielle_vermoegensgegenstaende numeric,
  aktivierte_eigenleistungen numeric,
  sachanlagen numeric,
  finanzanlagen numeric,
  umlaufvermoegen numeric,
  vorraete numeric,
  vorraete_vj numeric,
  forderungen numeric,
  forderungen_vj numeric,
  forderungen_lieferungen numeric,
  wertpapiere numeric,
  liquides_vermoegen numeric,
  liquides_vermoegen_vj numeric,
  rechnungsabgrenzungen numeric,
  eigenkapital numeric,
  eigenkapital_vj numeric,
  eingefordertes_stammkapital numeric,
  kapitalruecklagen numeric,
  gewinnruecklagen numeric,
  gewinnruecklagen_vj numeric,
  bilanzgewinn numeric,
  vortrag numeric,
  vortrag_vj numeric,
  rueckstellungen numeric,
  rueckstellungen_vj numeric,
  verbindlichkeiten numeric,
  verbindlichkeiten_vj numeric,
  langfristige_verbindlichkeiten numeric,
  kurzfristige_verbindlichkeiten numeric,
  verbindlichkeiten_lieferungen numeric,
  langfristige_forderungen numeric,
  kurzfristige_forderungen numeric,
  passive_rechnungsabgrenzungen numeric,
  raw_row jsonb,
  unique (financial_year_id)
);

create table if not exists ofb_financial_guv (
  id uuid primary key default gen_random_uuid(),
  financial_year_id uuid not null references ofb_financial_years(id) on delete cascade,
  betriebs_erfolg numeric,
  betriebs_erfolg_vj numeric,
  umsatzerloese numeric,
  umsatzerloese_vj numeric,
  waren_und_materialeinkauf numeric,
  waren_und_materialeinkauf_vj numeric,
  jahresueberschuss numeric,
  jahresueberschuss_vj numeric,
  bestandsveraenderung numeric,
  bestandsveraenderung_vj numeric,
  personalaufwand numeric,
  personalaufwand_vj numeric,
  steueraufwand numeric,
  ergebnis_vor_steuern numeric,
  zinsen_und_aehnliche_aufwendungen numeric,
  abschreibungen numeric,
  sonstige_betriebliche_ertraege numeric,
  soziale_aufwendungen numeric,
  sonstige_betriebliche_aufwendungen numeric,
  ertraege_aus_beteiligungen numeric,
  ertraege_aus_wertpapieren numeric,
  sonstige_zinsen_und_aehnliche_ertraege numeric,
  aufwendungen_aus_finanzanlagen numeric,
  finanzerfolg numeric,
  aufloesung_gewinnruecklagen numeric,
  raw_row jsonb,
  unique (financial_year_id)
);

create table if not exists ofb_financial_kennzahlen_bilanz (
  id uuid primary key default gen_random_uuid(),
  financial_year_id uuid not null references ofb_financial_years(id) on delete cascade,
  eigenkapitalquote numeric,
  fremdkapitalquote numeric,
  anlagendeckungsgrad numeric,
  anlagendeckungsgrad2 numeric,
  liquiditaet_grad1 numeric,
  liquiditaet_grad2 numeric,
  liquiditaet_grad3 numeric,
  working_capital numeric,
  anlagenintensitaet numeric,
  umlaufintensitaet numeric,
  verschuldungsgrad numeric,
  investiertes_kapital numeric,
  veraenderung_liquider_mittel numeric,
  return_on_equity numeric,
  return_on_equity_simplified numeric,
  return_on_assets numeric,
  return_on_assets_simplified numeric,
  raw_row jsonb,
  unique (financial_year_id)
);

create table if not exists ofb_financial_kennzahlen_guv (
  id uuid primary key default gen_random_uuid(),
  financial_year_id uuid not null references ofb_financial_years(id) on delete cascade,
  ebit_marge numeric,
  nettomarge numeric,
  materialquote numeric,
  personalquote numeric,
  umsatzwachstum_kurz numeric,
  effektiver_steuersatz numeric,
  investitionsquote numeric,
  debitoren_umschlagshaeufigkeit numeric,
  kreditoren_umschlagshaeufigkeit numeric,
  bruttomarge numeric,
  ausschuettungen numeric,
  return_on_invested_capital numeric,
  operativer_cashflow numeric,
  cashflow_quote numeric,
  lagerumschlagsdauer numeric,
  forderungsumschlagsdauer numeric,
  verbindlichkeitsdauer numeric,
  cash_conversion_cycle numeric,
  fcf numeric,
  capex numeric,
  raw_row jsonb,
  unique (financial_year_id)
);

-- cross-link table to map source datasets to resolved Firmenbuchnummer
create table if not exists ofb_company_source_links (
  id uuid primary key default gen_random_uuid(),
  firmennummer text not null,
  source_system text not null, -- wko | evi | projectfacts | manual
  source_key text not null,
  source_name text,
  confidence numeric,
  matched_at timestamptz default now(),
  unique (source_system, source_key),
  unique (firmennummer, source_system, source_key)
);

-- queue / matching / search indexes
create index if not exists ofb_crawl_queue_status_next_run_idx
on ofb_crawl_queue (status, next_run_at, priority);

create index if not exists ofb_crawl_queue_firmennummer_idx
on ofb_crawl_queue (firmennummer);

create index if not exists ofb_crawl_queue_search_name_trgm
on ofb_crawl_queue using gin (search_name gin_trgm_ops);

create index if not exists ofb_companies_court_idx
on ofb_companies (court_code);

create index if not exists ofb_companies_final_status_idx
on ofb_companies (final_status);

create index if not exists ofb_companies_final_names_idx
on ofb_companies using gin (final_names);

create index if not exists ofb_search_runs_ran_at_idx
on ofb_search_runs (ran_at desc);

create index if not exists ofb_search_results_firmennummer_idx
on ofb_search_results (firmennummer);

-- auszug indexes
create index if not exists ofb_auszug_snapshots_fnr_stichtag_idx
on ofb_auszug_snapshots (firmennummer, stichtag desc);

create unique index if not exists ofb_auszug_snapshots_unique_key_uq
on ofb_auszug_snapshots (firmennummer, stichtag, umfang);

create index if not exists ofb_auszug_vollz_snapshot_vnr_idx
on ofb_auszug_vollz (snapshot_id, vnr);

create index if not exists ofb_auszug_vollz_vollzugsdatum_idx
on ofb_auszug_vollz (vollzugsdatum desc);

create index if not exists ofb_auszug_euid_snapshot_idx
on ofb_auszug_euid (snapshot_id);

create unique index if not exists ofb_auszug_euid_snapshot_znr_euid_uq
on ofb_auszug_euid (snapshot_id, znr, euid);

create index if not exists ofb_auszug_euid_euid_idx
on ofb_auszug_euid (euid);

create index if not exists ofb_auszug_fun_snapshot_pnr_idx
on ofb_auszug_fun (snapshot_id, pnr);

create index if not exists ofb_auszug_fun_snapshot_fken_idx
on ofb_auszug_fun (snapshot_id, fken);

create index if not exists ofb_auszug_fun_dkz10_fun_vnr_idx
on ofb_auszug_fun_dkz10 (fun_id, vnr);

create index if not exists ofb_auszug_per_snapshot_pnr_idx
on ofb_auszug_per (snapshot_id, pnr);

create index if not exists ofb_auszug_per_dkz02_per_vnr_idx
on ofb_auszug_per_dkz02 (per_id, vnr);

create index if not exists ofb_auszug_firma_dkz02_snapshot_idx
on ofb_auszug_firma_dkz02 (snapshot_id);

create index if not exists ofb_auszug_firma_dkz03_snapshot_idx
on ofb_auszug_firma_dkz03 (snapshot_id);

create index if not exists ofb_auszug_firma_dkz06_snapshot_idx
on ofb_auszug_firma_dkz06 (snapshot_id);

create index if not exists ofb_auszug_firma_dkz07_snapshot_idx
on ofb_auszug_firma_dkz07 (snapshot_id);

-- financial indexes
create index if not exists ofb_financial_years_fnr_gj_idx
on ofb_financial_years (firmennummer, gj_ende desc);

create index if not exists ofb_company_source_links_firmennummer_idx
on ofb_company_source_links (firmennummer);
