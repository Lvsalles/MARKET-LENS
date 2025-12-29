DROP TABLE IF EXISTS stg_mls;

CREATE TABLE stg_mls (
    id SERIAL PRIMARY KEY,

    -- Controle
    project_id TEXT NOT NULL,
    source_file TEXT,
    category TEXT NOT NULL,   -- 'properties', 'land', 'rental'

    -- Identificação MLS
    ml_number TEXT,
    status TEXT,

    -- Endereço
    address TEXT,
    city TEXT,
    zipcode TEXT,
    subdivision TEXT,

    -- Características
    beds INTEGER,
    full_baths INTEGER,
    half_baths INTEGER,
    sqft NUMERIC,
    year_built INTEGER,

    -- Valores
    list_price NUMERIC,
    sold_price NUMERIC,
    rent_price NUMERIC,

    -- Métricas
    dom INTEGER,
    adom INTEGER,

    -- Datas
    list_date DATE,
    sold_date DATE,

    -- Metadados
    created_at TIMESTAMP DEFAULT NOW()
);
