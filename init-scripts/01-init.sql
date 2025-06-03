-- Создаем таблицы измерений
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    supplier_id INTEGER,
    supplier_name VARCHAR(255),
    supplier_country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(255),
    store_country VARCHAR(100),
    store_city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    order_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- Создаем таблицу фактов
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    date_id INTEGER,
    quantity INTEGER,
    total_amount DECIMAL(10,2),
    rating DECIMAL(3,2),
    review_count INTEGER,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
); 