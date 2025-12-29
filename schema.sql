-- =========================
-- Market Lens - Core Schema
-- =========================

create extension if not exists "pgcrypto";

-- 1) Projects (cada estudo/cliente)
create table if not exists projects (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null,
  name text not null,
  description text,
  created_at timestamptz default now()
);

create index if not exists idx_projects_owner on projects(owner_id);

-- 2) Datasets (cada upload)
create table if not exists datasets (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  filename text not null,
  source text default 'MLS',
  category text not null, -- Listings | Pendings | Sold | Land | Rental
  uploaded_at timestamptz default now(),
  status text default 'processing',
  record_count int default 0,
  notes text
);

create index if not exists idx_datasets_project on datasets(project_id);

-- 3) Raw records (cada linha original do Excel/CSV)
create table if not exists raw_records (
  id uuid primary key default gen_random_uuid(),
  dataset_id uuid not null references datasets(id) on delete cascade,
  row_num int not null,
  raw jsonb not null,
  inserted_at timestamptz default now()
);

create index if not exists idx_raw_dataset on raw_records(dataset_id);

-- 4) Normalized properties (1 registro por imóvel/linha)
create table if not exists normalized_properties (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  dataset_id uuid not null references datasets(id) on delete cascade,

  -- identity / location
  mls_id text,
  address text,
  city text,
  county text,
  state text default 'FL',
  zipcode text,

  -- dates
  list_date date,
  pending_date date,
  sold_date date,

  -- status/category
  category text not null,  -- Listings | Pendings | Sold | Land | Rental
  status text,             -- Active | Pending | Sold etc.
  property_type text,      -- Single Family, Condo, Land...
  property_subtype text,

  -- numbers
  price numeric,
  sold_price numeric,
  sqft numeric,
  lot_sqft numeric,

  beds int,
  baths numeric,
  garage int,

  year_built int,

  dom int,   -- Days on Market (CDOM/ADOM - escolha um padrão)
  adom int,

  -- finance / metadata
  financing text,
  hoa numeric,

  -- agents (para top 20)
  list_agent text,
  list_office text,
  sell_agent text,
  sell_office text,

  -- derived fields
  ppsqft numeric,         -- price per sqft (list)
  sp_psqft numeric,       -- sold price per sqft
  month_key date,         -- primeiro dia do mês (para MoM/YoY)

  inserted_at timestamptz default now()
);

create index if not exists idx_norm_project on normalized_properties(project_id);
create index if not exists idx_norm_zip on normalized_properties(project_id, zipcode);
create index if not exists idx_norm_month on normalized_properties(project_id, month_key);
create index if not exists idx_norm_category on normalized_properties(project_id, category);

-- 5) Metrics facts (cache)
create table if not exists metrics_facts (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  category text not null,
  scope text not null,         -- overall | zipcode
  scope_value text not null,   -- 'ALL' or zipcode
  month_key date not null,     -- para MoM/YoY
  record_count int not null,

  avg_price numeric,
  avg_sqft numeric,
  avg_ppsqft numeric,
  avg_adom numeric,

  avg_beds numeric,
  avg_baths numeric,
  avg_garage numeric,

  created_at timestamptz default now(),

  unique(project_id, category, scope, scope_value, month_key)
);

create index if not exists idx_facts_project on metrics_facts(project_id, category, scope, month_key);
