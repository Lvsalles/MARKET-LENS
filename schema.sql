create extension if not exists "pgcrypto";

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
  category text not null,
  status text default 'processing',
  record_count int default 0,
  uploaded_at timestamptz default now()
);

create table if not exists normalized_properties (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references projects(id) on delete cascade,
  dataset_id uuid not null references datasets(id) on delete cascade,
  category text not null,

  mls_id text,
  address text,
  city text,
  zipcode text,

  property_type text,
  property_subtype text,
  financing text,

  price numeric,
  sold_price numeric,
  sqft numeric,

  beds numeric,
  baths numeric,
  garage numeric,

  dom numeric,
  adom numeric,

  list_date date,
  sold_date date,

  ppsqft numeric,
  month_key date,

  list_agent text,
  sell_agent text,

  inserted_at timestamptz default now()
);

create index if not exists idx_np_project_cat on normalized_properties(project_id, category);
create index if not exists idx_np_zip on normalized_properties(project_id, zipcode);
create index if not exists idx_np_month on normalized_properties(project_id, month_key);
