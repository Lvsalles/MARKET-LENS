-- =========================
-- PROJECTS
-- =========================
create table if not exists projects (
  project_id text primary key,
  created_at timestamptz default now()
);

-- =========================
-- UPLOAD LOG
-- =========================
create table if not exists uploads (
  upload_id uuid default gen_random_uuid() primary key,
  project_id text references projects(project_id),
  dataset_type text check (dataset_type in ('properties','land','rental')),
  filename text,
  rows_loaded int,
  created_at timestamptz default now()
);

-- =========================
-- STAGING (RAW DATA)
-- =========================
create table if not exists stg_raw (
  raw_id uuid default gen_random_uuid() primary key,
  project_id text,
  dataset_type text,
  status text,
  data jsonb,
  natural_key text,
  created_at timestamptz default now(),
  unique (project_id, dataset_type, natural_key)
);
