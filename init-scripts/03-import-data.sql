-- Создаем временную таблицу для импорта данных
CREATE TABLE IF NOT EXISTS temp_mock_data (
    order_id INTEGER,
    customer_id INTEGER,
    customer_name VARCHAR(255),
    customer_country VARCHAR(100),
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    supplier_id INTEGER,
    supplier_name VARCHAR(255),
    supplier_country VARCHAR(100),
    store_id INTEGER,
    store_name VARCHAR(255),
    store_country VARCHAR(100),
    store_city VARCHAR(100),
    order_date DATE,
    quantity INTEGER,
    total_amount DECIMAL(10,2),
    rating DECIMAL(3,2),
    review_count INTEGER
);

-- Импортируем данные из CSV файлов
COPY temp_mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA*.csv' WITH (FORMAT csv, HEADER true);

-- Заполняем таблицы измерений
INSERT INTO dim_products (product_id, product_name, category, price, supplier_id, supplier_name, supplier_country)
SELECT DISTINCT product_id, product_name, category, price, supplier_id, supplier_name, supplier_country
FROM temp_mock_data;

INSERT INTO dim_customers (customer_id, customer_name, customer_country)
SELECT DISTINCT customer_id, customer_name, customer_country
FROM temp_mock_data;

INSERT INTO dim_stores (store_id, store_name, store_country, store_city)
SELECT DISTINCT store_id, store_name, store_country, store_city
FROM temp_mock_data;

INSERT INTO dim_dates (order_date, year, month, day)
SELECT DISTINCT order_date, EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date), EXTRACT(DAY FROM order_date)
FROM temp_mock_data;

-- Заполняем таблицу фактов
INSERT INTO fact_sales (order_id, customer_id, product_id, store_id, date_id, quantity, total_amount, rating, review_count)
SELECT 
    t.order_id,
    t.customer_id,
    t.product_id,
    t.store_id,
    d.date_id,
    t.quantity,
    t.total_amount,
    t.rating,
    t.review_count
FROM temp_mock_data t
JOIN dim_dates d ON t.order_date = d.order_date;

-- Удаляем временную таблицу
DROP TABLE temp_mock_data; 