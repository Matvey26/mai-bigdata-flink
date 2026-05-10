-- Flink SQL job: kafka (json) -> star schema in postgres
-- Запуск: docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/flink_job.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'sales-to-star';
-- иначе jdbc sink не флашится в БД
SET 'execution.checkpointing.interval' = '5s';

-- источник kafka
CREATE TABLE sales_src (
    id INT,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price DOUBLE,
    product_quantity INT,
    sale_date STRING,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price DOUBLE,
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight DOUBLE,
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DOUBLE,
    product_reviews INT,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sales',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- синки postgres
CREATE TABLE dim_customer_sink (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    age INT,
    email STRING,
    country STRING,
    postal_code STRING,
    pet_type STRING,
    pet_name STRING,
    pet_breed STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab',
    'table-name' = 'dim_customer',
    'username' = 'lab',
    'password' = 'lab'
);

CREATE TABLE dim_seller_sink (
    seller_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    country STRING,
    postal_code STRING,
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab',
    'table-name' = 'dim_seller',
    'username' = 'lab',
    'password' = 'lab'
);

CREATE TABLE dim_product_sink (
    product_id INT,
    name STRING,
    category STRING,
    price DOUBLE,
    weight DOUBLE,
    color STRING,
    size STRING,
    brand STRING,
    material STRING,
    description STRING,
    rating DOUBLE,
    reviews INT,
    release_date STRING,
    expiry_date STRING,
    pet_category STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab',
    'table-name' = 'dim_product',
    'username' = 'lab',
    'password' = 'lab'
);

CREATE TABLE fact_sales_sink (
    sale_date STRING,
    customer_id INT,
    seller_id INT,
    product_id INT,
    quantity INT,
    total_price DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab',
    'table-name' = 'fact_sales',
    'username' = 'lab',
    'password' = 'lab'
);

-- запуск всех инсертов одним стейтментом
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO dim_customer_sink
    SELECT sale_customer_id, customer_first_name, customer_last_name,
           customer_age, customer_email, customer_country,
           customer_postal_code, customer_pet_type,
           customer_pet_name, customer_pet_breed
    FROM sales_src;

    INSERT INTO dim_seller_sink
    SELECT sale_seller_id, seller_first_name, seller_last_name,
           seller_email, seller_country, seller_postal_code
    FROM sales_src;

    INSERT INTO dim_product_sink
    SELECT sale_product_id, product_name, product_category, product_price,
           product_weight, product_color, product_size, product_brand,
           product_material, product_description, product_rating,
           product_reviews, product_release_date, product_expiry_date,
           pet_category
    FROM sales_src;

    INSERT INTO fact_sales_sink
    SELECT sale_date, sale_customer_id, sale_seller_id, sale_product_id,
           sale_quantity, sale_total_price
    FROM sales_src;
END;
