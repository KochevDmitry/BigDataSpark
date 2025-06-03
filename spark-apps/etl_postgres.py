from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL PostgreSQL") \
        .config("spark.jars", "/opt/bitnami/spark/external-jars/postgresql-42.7.1.jar") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

def read_source_data(spark):
    # Читаем все CSV файлы из директории
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/MOCK_DATA*.csv")

def create_dimension_tables(df):
    # Таблица продуктов
    dim_products = df.select(
        "sale_product_id",  # идентификатор продукта в продаже
        "product_name",
        "product_category",
        "product_price",
        "product_brand",
        "product_size",
        "product_color",
        "product_weight",
        "product_material",
        "product_description",
        "product_release_date",
        "product_expiry_date"
    ).distinct()

    # Таблица покупателей
    dim_customers = df.select(
        "sale_customer_id",  # идентификатор покупателя в продаже
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_age",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed"
    ).distinct()

    # Таблица магазинов
    dim_stores = df.select(
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    ).distinct()

    # Таблица дат
    dim_dates = df.select(
        "sale_date"
    ).distinct() \
        .withColumn("date_id", monotonically_increasing_id()) \
        .withColumn("year", year("sale_date")) \
        .withColumn("month", month("sale_date")) \
        .withColumn("day", dayofmonth("sale_date"))

    return dim_products, dim_customers, dim_stores, dim_dates

def create_fact_table(df, dim_dates):
    # Таблица фактов продаж
    fact_sales = df.join(
        dim_dates,
        df.sale_date == dim_dates.sale_date,
        "left"
    ).select(
        "id",  # уникальный идентификатор строки
        "sale_customer_id",
        "sale_product_id",
        "store_name",
        "date_id",
        "sale_quantity",
        "sale_total_price",
        "product_rating",
        "product_reviews"
    )
    return fact_sales

def write_to_postgres(df, table_name, mode="append"):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()

def clear_tables(spark):
    # Очищаем таблицы в правильном порядке
    tables = [
        "fact_sales",
        "dim_products",
        "dim_customers",
        "dim_stores",
        "dim_dates"
    ]
    
    for table in tables:
        # Создаем пустой DataFrame с правильной структурой
        empty_df = spark.createDataFrame([], spark.table(table).schema)
        
        # Записываем пустой DataFrame, перезаписывая таблицу
        empty_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
            .option("dbtable", table) \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

def main():
    spark = create_spark_session()
    
    # Читаем исходные данные
    source_df = read_source_data(spark)
    
    # Создаем таблицы измерений
    dim_products, dim_customers, dim_stores, dim_dates = create_dimension_tables(source_df)
    
    # Создаем таблицу фактов
    fact_sales = create_fact_table(source_df, dim_dates)
    
    # Сначала записываем данные в PostgreSQL в правильном порядке
    write_to_postgres(dim_dates, "dim_dates", "overwrite")
    write_to_postgres(dim_products, "dim_products", "overwrite")
    write_to_postgres(dim_customers, "dim_customers", "overwrite")
    write_to_postgres(dim_stores, "dim_stores", "overwrite")
    write_to_postgres(fact_sales, "fact_sales", "overwrite")
    
    spark.stop()

if __name__ == "__main__":
    main() 