# Data Table Insights

This document explains the database tables in detail: what each table stores, why each field exists, and what the table can tell you analytically.

The schemas are defined in:
- `scripts/setup_wko_schema.sql`
- `scripts/setup_projectfacts_schema.sql`
- `scripts/setup_evi_schema.sql`

---

## Overall Data Model

The data model represents three different but complementary sources:

- `wko_*` tables: company directory data from WKO branch pages.
- `projectfacts`: company master-like profile records imported from spreadsheet data.
- `evi_bilanz_publications`: legal/publication events (financial publication notices) from EVI.

At a high level:
- `wko_companies` tells you **who appears in branch directories and how to contact them**.
- `wko_branches` tells you **which branch categories exist and where they were discovered**.
- `projectfacts` tells you **structured company descriptors and normalized profile attributes**.
- `evi_bilanz_publications` tells you **which companies had publication events and when**.

There are no explicit foreign-key relationships across these tables, so cross-source matching is done logically (typically by normalized company name, address, or IDs like `firmenbuchnummer` when available in a source).

---

## Table: `wko_branches`

### What it contains

`wko_branches` is the branch taxonomy catalog used by the crawler. Each row describes one branch label plus its source URL in WKO.

This is the directory backbone: it defines the crawl universe and the categorization context for `wko_companies`.

### Columns and meaning

- `id`: technical UUID primary key.
- `branche`: branch name/category label as exposed on WKO.
- `branch_url`: URL for that specific branch listing.
- `letter`: alphabet bucket used during branch discovery (A-Z crawl process).
- `source`: source URL used for discovery (typically branch index page).
- `discovered_at`: timestamp of branch discovery batch.
- `created_at`, `updated_at`: warehouse-style technical timestamps.

### Constraints and indexes

- Unique branch identity: `(branche, branch_url)` is unique.
- Indexes emphasize:
  - deduplication and lookup by `(branche, branch_url)`,
  - filtering by `letter`.

### What this table tells you

- Coverage of the branch universe your crawler can target.
- Stability/changes in branch taxonomy over time (if periodically refreshed).
- Crawl segmentation: how categories are distributed across alphabet buckets.
- Which branches exist even before company-level crawling has happened.

### Typical interpretation

If a branch exists here but has no downstream `wko_companies` growth, it may indicate:
- low-yield branch categories,
- crawl access problems,
- or branches that currently list no entries.

---

## Table: `wko_companies`

### What it contains

`wko_companies` stores company-level records extracted from WKO branch result pages. It is the central operational directory table for WKO-sourced entities.

Each row is an enriched crawl artifact: human-readable fields, normalized text support (`search_text`), and original payload (`raw_row`).

### Columns and meaning

- `id`: technical UUID primary key.
- `wko_key`: deterministic unique hash key used for upserts and dedupe.
- `branche`: branch category where company was observed.
- `name`: company name as scraped.
- `wko_detail_url`: canonical detail/listing URL on WKO.
- `company_website`: public website if available.
- `email`: email if available.
- `phone`: phone if available.
- `street`, `zip_city`, `address`: granular and combined location text.
- `source_list_url`: listing page where the row was found.
- `crawled_at`: time observed by crawler.
- `imported_at`: time imported/upserted into DB.
- `search_text`: normalized multi-field search string for fuzzy search.
- `raw_row`: raw JSON payload from crawler/import stage.
- `created_at`, `updated_at`: technical timestamps.

### Constraints and indexes

- Unique business key: `wko_key` (upsert conflict key).
- Additional indexes support:
  - URL lookup (`wko_detail_url`),
  - branch-based filtering (`branche`),
  - recency filtering (`crawled_at`),
  - fuzzy matching via trigram GIN on `name`, `address`, `search_text`.

### What this table tells you

- Which companies are discoverable in the WKO ecosystem.
- Contact reachability density by branch (website/email/phone completeness).
- Geographic text patterns from addresses and city/postcode strings.
- Crawl recency and freshness via `crawled_at`.
- Branch-level directory yield and discoverability.

### Data quality behavior to expect

- Missing contact fields are normal (not all listings expose all channels).
- Duplicate real-world companies can appear under multiple branches.
- Address text can vary in formatting; `search_text` exists to reduce matching friction.
- Since dedupe key is hash-based from normalized identity inputs, key design affects merge behavior.

### Typical interpretation

This table is strongest for:
- lead universe exploration,
- branch-segmented contact enrichment,
- fuzzy entity lookup by name/address text.

It is less suitable as a strict legal entity master without cross-source reconciliation.

---

## Table: `projectfacts`

### What it contains

`projectfacts` is a structured imported company profile table originating from spreadsheet-based source data. It looks like a curated business directory with normalization and analytics-ready text fields.

Compared to `wko_companies`, this table is richer in profile attributes (industry, company size, segmentation country, activity timestamps).

### Columns and meaning

- `id`: technical UUID primary key.
- `pf_key`: deterministic unique key for row identity/upsert.
- `name`, `ort`: core descriptive company labels from source.
- `name_norm`: normalized name for matching/search.
- `street`, `plz`, `city`, `city_norm`, `state`, `country`: location fields.
- `segment_country`: country segmentation attribute from source business logic.
- `industries`: industry classification text.
- `size`: company size bucket/text.
- `last_changed_at`: source-side "last changed" timestamp.
- `last_activity_at`: source-side "last activity" timestamp.
- `company_address`: full company address string.
- `raw_addresses`: source address blob/multi-address text.
- `address_norm`: normalized address representation for matching.
- `search_text`: normalized aggregate search field.
- `raw_row`: raw imported source row for lineage and debugging.
- `created_at`, `updated_at`: technical timestamps.

### Constraints and indexes

- Unique key: `pf_key`.
- Trigram indexes on `name`, `company_address`, `city`, `search_text` optimize fuzzy/entity search and broad keyword matching.

### What this table tells you

- A normalized company profile layer with richer segmentation than pure crawl output.
- Industry/size patterns for target segmentation.
- Geo-distribution with standardized city normalization.
- Behavioral freshness signals through `last_changed_at` and `last_activity_at`.

### Data quality behavior to expect

- Importer maps German source headers to canonical fields, so column mapping quality is critical.
- Text normalization is intentionally aggressive for matching (umlauts, punctuation, whitespace normalization).
- Same real entity may still duplicate if source rows differ enough to alter `pf_key` inputs.

### Typical interpretation

This table is best for:
- ICP segmentation (industry/size/geo),
- profile-driven filtering,
- joining candidate entities to other sources via normalized name/address strategies.

---

## Table: `evi_bilanz_publications`

### What it contains

`evi_bilanz_publications` stores publication notices from EVI search results (focused on "Bilanz" in crawler defaults). It is an event/publication table, not a static master table.

Each row is a publication event linked to a company label and legal identifier text when available.

### Columns and meaning

- `id`: technical UUID primary key.
- `evi_key`: deterministic unique key used for dedupe/upsert.
- `publication_date`: publication date (date type).
- `publication_type`: publication category/type text.
- `detail_url`: publication detail URL.
- `source_item_path`: relative source path used during extraction.
- `source_search_url`: search result URL/page context.
- `company_name`: company name as displayed in publication card.
- `company_name_norm`: normalized company name for matching/search.
- `firmenbuchnummer`: firm register number when present.
- `search_text`: normalized aggregate text for search.
- `crawled_at`: crawl observation timestamp.
- `imported_at`: DB import timestamp.
- `raw_row`: raw extracted payload for lineage.
- `created_at`, `updated_at`: technical timestamps.

### Constraints and indexes

- Unique keys on `evi_key` and `detail_url` protect against duplicate event ingestion.
- Indexes for:
  - recency/range analysis (`publication_date`),
  - legal-id lookup (`firmenbuchnummer`),
  - fuzzy search on company and text (`company_name`, `search_text` via trigram GIN).

### What this table tells you

- Legal/publication event activity by company over time.
- Temporal distribution of publication events.
- Potential legal-identifier-based linkage opportunities (`firmenbuchnummer`) to other systems.
- Freshness and backfill patterns in event ingestion.

### Data quality behavior to expect

- `publication_date` can be absent if extraction fails on a specific card format.
- Company name formatting may include legal suffix variability.
- `firmenbuchnummer` presence is highly valuable but not guaranteed for every row.

### Typical interpretation

This table is ideal for:
- event timeline analyses,
- publication monitoring use cases,
- identifying entities with recent financial publication activity.

---

## Cross-Table Meaning and Reconciliation Strategy

Since no hard foreign keys are defined between source families, practical integration is probabilistic/rule-based:

- `wko_companies` <-> `projectfacts`
  - primary bridge: normalized `name` + normalized address fields.
  - support bridge: city/postcode + fuzzy text similarity.

- `projectfacts` <-> `evi_bilanz_publications`
  - strongest bridge: `firmenbuchnummer` (if available externally in both systems).
  - fallback bridge: normalized company name and temporal plausibility checks.

- `wko_companies` <-> `evi_bilanz_publications`
  - weaker direct bridge due to limited shared explicit IDs; use name normalization and possibly domain/website enrichment.

The model therefore supports a layered architecture:
- source-native tables remain clean and append/upsert friendly,
- resolved "golden entity" views can be built later on top.

---

## What the Whole Dataset Tells You

Taken together, the tables describe:

- **Market visibility**: which companies are visible in WKO by branch (`wko_companies`, `wko_branches`).
- **Business profile depth**: how those companies can be segmented (`projectfacts`).
- **Regulatory/publication activity**: which entities show publication events and when (`evi_bilanz_publications`).

This enables combined use cases such as:
- finding companies in specific industries/regions with recent publication activity,
- building prioritized lead lists by branch + profile criteria,
- tracking freshness of crawl and import cycles,
- validating entity consistency across public sources.

---

## Practical Caveats

- Source heterogeneity means name/address normalization quality drives match success.
- Hash-based keys are source-specific dedupe keys, not universal company IDs.
- Temporal fields (`crawled_at`, `imported_at`, `last_*`) should be interpreted by source semantics, not as interchangeable timestamps.
- `raw_row` columns are valuable for auditability and parser evolution but should not be treated as stable schema contracts.

---

## Recommended Next Layer

For analytics and product usage, create derived views/materialized tables:

- `entity_candidates`:
  - normalized entity records from all sources with confidence scores.
- `entity_activity_timeline`:
  - merged event timeline from crawl freshness + EVI publication dates.
- `lead_scoring_view`:
  - combines branch relevance, profile segmentation, and recent publication signals.

This keeps ingestion tables source-faithful while giving you business-ready outputs.

