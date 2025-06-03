# BigDataSpark - ETL с использованием Apache Spark

Этот проект реализует ETL-пайплайн с использованием Apache Spark для анализа данных о продажах.

## Требования

- Docker
- Docker Compose
- Python 3.8+
- DBeaver (или другой SQL-клиент)

## Структура проекта

```
.
├── docker-compose.yml
├── requirements.txt
├── init-scripts/
│   ├── 01-init.sql
│   └── 02-init-clickhouse.sql
├── spark-apps/
│   ├── etl_postgres.py
│   └── clickhouse_reports.pyhttps://github.com/box1t/BigDataSpark
└── исходные данные/
    └── MOCK_DATA*.csv
```

## Запуск проекта

1. Запустите все сервисы с помощью Docker Compose:
```bash
docker-compose up -d
```

2. Дождитесь инициализации всех сервисов (это может занять несколько минут).

3. Запустите ETL-процесс для PostgreSQL:
```bash
docker exec -it bigdataspark_spark_1 spark-submit --master spark://spark:7077 --conf spark.jars.ivy=/tmp --jars /opt/bitnami/spark/external-jars/postgresql-42.7.1.jar /opt/bitnami/spark/apps/etl_postgres.py
```

4. Запустите создание отчетов в ClickHouse:
```bash
docker exec -i bigdataspark_clickhouse_1 clickhouse-client < init-scripts/02-init-clickhouse.sql

docker exec -it bigdataspark_spark_1 spark-submit --master spark://spark:7077 --conf spark.jars.ivy=/tmp --jars /opt/bitnami/spark/external-jars/postgresql-42.7.1.jar,/opt/bitnami/spark/external-jars/clickhouse-jdbc-0.4.6.jar /opt/bitnami/spark/apps/clickhouse_reports.py
```

## Проверка результатов

### PostgreSQL
Подключитесь к PostgreSQL через DBeaver:
- Host: localhost
- Port: 5432
- Database: bigdata
- Username: postgres
- Password: postgres

### ClickHouse
Подключитесь к ClickHouse через DBeaver:
- Host: localhost
- Port: 8123
- Database: default
- Username: default
- Password: (пустой)

## Доступные отчеты

### 1. Витрина продаж по продуктам
- Таблицы: `top_products`, `category_revenue`

### 2. Витрина продаж по клиентам
- Таблицы: `top_customers`, `country_distribution`

### 3. Витрина продаж по времени
- Таблица: `time_trends`

### 4. Витрина продаж по магазинам
- Таблицы: `top_stores`, `store_distribution`

### 5. Витрина продаж по поставщикам
- Таблицы: `top_suppliers`, `supplier_country_distribution`

### 6. Витрина качества продукции
- Таблица: `product_ratings`

## Остановка проекта

Для остановки всех сервисов выполните:
```bash
docker-compose down
```

Для полного удаления данных (включая тома) выполните:
```bash
docker-compose down -v
```
