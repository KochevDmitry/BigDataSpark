-- Создаем таблицы для отчетов

-- Отчет по продуктам
CREATE TABLE IF NOT EXISTS top_products (
    sale_product_id Int32,
    product_name String,
    total_quantity Int32,
    total_revenue Float64,
    avg_rating Float64,
    total_reviews Int32
) ENGINE = MergeTree()
ORDER BY (sale_product_id);

CREATE TABLE IF NOT EXISTS category_revenue (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY (product_category);

-- Отчет по клиентам
CREATE TABLE IF NOT EXISTS top_customers (
    sale_customer_id Int32,
    customer_name String,
    total_spent Float64,
    total_orders Int32,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (sale_customer_id);

CREATE TABLE IF NOT EXISTS country_distribution (
    customer_country String,
    customer_count Int32,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY (customer_country);

-- Отчет по времени
CREATE TABLE IF NOT EXISTS time_trends (
    year Int32,
    month Int32,
    monthly_revenue Float64,
    avg_order_value Float64,
    order_count Int32
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Отчет по магазинам
CREATE TABLE IF NOT EXISTS top_stores (
    store_name String,
    total_revenue Float64,
    order_count Int32,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (store_name);

CREATE TABLE IF NOT EXISTS store_distribution (
    store_country String,
    store_city String,
    total_revenue Float64,
    store_count Int32
) ENGINE = MergeTree()
ORDER BY (store_country, store_city);

-- Отчет по поставщикам
CREATE TABLE IF NOT EXISTS top_suppliers (
    supplier_id UInt32,
    supplier_name String,
    total_revenue Decimal(10,2),
    avg_product_price Decimal(10,2),
    product_count UInt32
) ENGINE = MergeTree()
ORDER BY (supplier_id);

CREATE TABLE IF NOT EXISTS supplier_country_distribution (
    supplier_country String,
    total_revenue Decimal(10,2),
    supplier_count UInt32
) ENGINE = MergeTree()
ORDER BY (supplier_country);

-- Отчет по качеству продукции
CREATE TABLE IF NOT EXISTS product_ratings (
    sale_product_id Int32,
    product_name String,
    avg_rating Float64,
    total_reviews Int32,
    total_quantity Int32
) ENGINE = MergeTree()
ORDER BY (sale_product_id); 