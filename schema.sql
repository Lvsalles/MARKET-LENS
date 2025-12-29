-- =========================================
-- Market Lens - Schema v2 (Phase 1)
-- Supabase Postgres
-- =========================================

create extension if not exists "pgcrypto";

-- -------------------------
-- Projects / Datasets
-- -------------------------
create table if not exists projects (
  id uuid primary key default gen_random_uuid(),
  owner_id text not null,
  name text not null,
  created_at timestamptz default now(),
  unique(owner_id, name)
);

create table if not exists datasets (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  filename text not null,
  category text not null,        -- Listings, Pendings, Sold, Land, Rental
  status text default 'processing',
  record_count int default 0,
  uploaded_at timestamptz default now()
);

create index if not exists idx_datasets_project on datasets(project_id);

-- -------------------------
-- STAGING (recebe o Excel normalizado)
-- -------------------------
create table if not exists stg_mls (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  dataset_id uuid not null references datasets(id) on delete cascade,
  category text not null,

  -- Identificação
  mls_id text,
  address text,
  street text,                -- importante p/ Streets 2+
  city text,
  state text,
  zipcode text,
  subdivision text,

  -- Segmentação / Tipo de ativo
  property_style text,        -- SFR, Condo, Townhouse, Manufactured...
  property_type text,
  property_subtype text,

  -- Principais atributos
  beds numeric,
  baths numeric,
  garage numeric,
  pool boolean,
  year_built int,

  -- Valores
  list_price numeric,
  sold_price numeric,
  sqft numeric,
  lot_sqft numeric,

  -- Métricas de mercado
  dom numeric,
  adom numeric,
  cdom numeric,
  sp_lp numeric,              -- sold_price / list_price (Sold)
  ppsqft numeric,             -- price per sqft (list/sold)

  -- Datas
  list_date date,
  pending_date date,
  sold_date date,
  month_key date,             -- YYYY-MM-01 para MoM/YoY

  -- Financing / agentes
  financing text,             -- Cash/FHA/VA/Conventional/Other
  list_agent text,
  sell_agent text,

  -- Geografia opcional (para map intelligence depois)
  latitude numeric,
  longitude numeric,

  inserted_at timestamptz default now()
);

create index if not exists idx_stg_project_cat on stg_mls(project_id, category);
create index if not exists idx_stg_zip on stg_mls(project_id, zipcode);
create index if not exists idx_stg_street on stg_mls(project_id, street);
create index if not exists idx_stg_month on stg_mls(project_id, month_key);

-- -------------------------
-- DIMENSIONS (modelo final)
-- -------------------------

-- dim_location: ZIP/cidade/condado + geo
create table if not exists dim_location (
  id uuid primary key default gen_random_uuid(),
  zipcode text,
  city text,
  county text,
  state text,
  latitude numeric,
  longitude numeric,
  created_at timestamptz default now(),
  unique(zipcode, city, state)
);

create index if not exists idx_dim_location_zip on dim_location(zipcode);

-- dim_property: atributos físicos
create table if not exists dim_property (
  id uuid primary key default gen_random_uuid(),
  property_style text,
  property_type text,
  property_subtype text,
  beds numeric,
  baths numeric,
  garage numeric,
  pool boolean,
  year_built int,
  sqft numeric,
  lot_sqft numeric,
  subdivision text,
  created_at timestamptz default now()
);

create index if not exists idx_dim_property_style on dim_property(property_style);
create index if not exists idx_dim_property_year on dim_property(year_built);

-- dim_time: datas (MoM/YoY)
create table if not exists dim_time (
  id uuid primary key default gen_random_uuid(),
  date_value date not null,
  year int not null,
  month int not null,
  month_key date not null,       -- YYYY-MM-01
  created_at timestamptz default now(),
  unique(date_value)
);

create index if not exists idx_dim_time_monthkey on dim_time(month_key);

-- dim_financing: padroniza
create table if not exists dim_financing (
  id uuid primary key default gen_random_uuid(),
  financing text not null,
  created_at timestamptz default now(),
  unique(financing)
);

-- dim_zoning: (fase 1 cria tabela; fase 5 preenchimento)
create table if not exists dim_zoning (
  id uuid primary key default gen_random_uuid(),
  zoning_code text,
  jurisdiction text,           -- City vs County
  adu_allowed text,            -- Yes/No/Conditional
  rules_url text,
  notes text,
  created_at timestamptz default now(),
  unique(zoning_code, jurisdiction)
);

-- -------------------------
-- FACT TABLES (modelo final)
-- -------------------------

-- Listings (ACT)
create table if not exists fact_listings (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  dataset_id uuid not null references datasets(id) on delete cascade,

  property_id uuid references dim_property(id),
  location_id uuid references dim_location(id),
  time_id uuid references dim_time(id),
  financing_id uuid references dim_financing(id),
  zoning_id uuid references dim_zoning(id),

  mls_id text,
  address text,
  street text,

  list_price numeric,
  sqft numeric,
  ppsqft numeric,
  dom numeric,
  adom numeric,
  created_at timestamptz default now()
);

create index if not exists idx_fact_listings_project on fact_listings(project_id);
create index if not exists idx_fact_listings_zip on fact_listings(project_id, location_id);

-- Pending (PND)
create table if not exists fact_pending (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  dataset_id uuid not null references datasets(id) on delete cascade,

  property_id uuid references dim_property(id),
  location_id uuid references dim_location(id),
  time_id uuid references dim_time(id),
  financing_id uuid references dim_financing(id),

  mls_id text,
  address text,
  street text,

  list_price numeric,
  sqft
