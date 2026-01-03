-- =========================
-- STAGING TABLE (MLS RAW)
-- =========================

create table if not exists public.stg_mls (
    id bigserial primary key,

    project_id text not null,

    -- Identificação
    mls_number text,
    address text,
    city text,
    zip text,
    county text,
    subdivision text,

    -- Tipo e status
    property_type text,
    status text,
    status_norm text,

    -- Preço
    current_price numeric,
    original_price numeric,
    sold_price numeric,

    -- Dimensões
    heated_area numeric,
    lot_size numeric,

    -- Características
    beds numeric,
    full_baths numeric,
    half_baths numeric,
    year_built integer,
    garage_spaces integer,
    pool boolean,

    -- Mercado
    adom integer,
    cdom integer,
    sp_lp numeric,

    -- Datas
    list_date date,
    pending_date date,
    sold_date date,

    -- Geografia (futuro Google Maps)
    latitude numeric,
    longitude numeric,

    -- Controle
    created_at timestamp default now()
);

create index if not exists idx_stg_mls_project
on public.stg_mls(project_id);

create index if not exists idx_stg_mls_status
on public.stg_mls(status_norm);
