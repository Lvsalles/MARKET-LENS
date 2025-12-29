CREATE TABLE IF NOT EXISTS stg_mls (
    id SERIAL PRIMARY KEY,
    ml_number TEXT,
    status TEXT,
    address TEXT,
    city TEXT,
    zipcode TEXT,
    legal_subdivision_name TEXT,
    heated_area INTEGER,
    current_price NUMERIC,
    beds INTEGER,
    full_baths INTEGER,
    half_baths INTEGER,
    year_built INTEGER,
    pool BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW()
);
