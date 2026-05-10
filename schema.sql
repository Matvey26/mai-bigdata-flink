-- Слегка упростил схему для этой лабы

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     INT PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    age             INT,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT,
    pet_type        TEXT,
    pet_name        TEXT,
    pet_breed       TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id       INT PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          INT PRIMARY KEY,
    name                TEXT,
    category            TEXT,
    price               NUMERIC(12,2),
    weight              NUMERIC(12,2),
    color               TEXT,
    size                TEXT,
    brand               TEXT,
    material            TEXT,
    description         TEXT,
    rating              NUMERIC(4,2),
    reviews             INT,
    release_date        TEXT,
    expiry_date         TEXT,
    pet_category        TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id        SERIAL PRIMARY KEY,
    name            TEXT,
    location        TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    phone           TEXT,
    email           TEXT,
    UNIQUE (name, city, country)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id     SERIAL PRIMARY KEY,
    name            TEXT,
    contact         TEXT,
    email           TEXT,
    phone           TEXT,
    address         TEXT,
    city            TEXT,
    country         TEXT,
    UNIQUE (name, email)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    sale_date       TEXT,
    customer_id     INT,
    seller_id       INT,
    product_id      INT,
    quantity        INT,
    total_price     NUMERIC(14,2)
);
