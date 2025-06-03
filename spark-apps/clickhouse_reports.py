from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ClickHouse Reports") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()

def read_from_postgres(spark, table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

def create_product_sales_report(fact_sales, dim_products):
    dim_products_renamed = dim_products.select(
        col("sale_product_id").alias("product_id_dim"),
        col("product_name"),
        col("product_category"),
        col("product_price")
    )
    sales_with_products = fact_sales.join(
        dim_products_renamed,
        fact_sales.sale_product_id == dim_products_renamed.product_id_dim,
        "left"
    )
    top_products = sales_with_products \
        .groupBy("sale_product_id", "product_name") \
        .agg(
            sum("sale_quantity").alias("total_quantity"),
            sum("sale_total_price").alias("total_revenue"),
            avg("product_rating").alias("avg_rating"),
            sum("product_reviews").alias("total_reviews")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(10)
    category_revenue = sales_with_products \
        .groupBy("product_category") \
        .agg(
            sum("sale_total_price").alias("total_revenue")
        ) \
        .orderBy(desc("total_revenue"))
    return top_products, category_revenue

def create_customer_sales_report(fact_sales, dim_customers):
    dim_customers_renamed = dim_customers.select(
        col("sale_customer_id").alias("customer_id_dim"),
        col("customer_first_name"),
        col("customer_last_name"),
        col("customer_country")
    )
    sales_with_customers = fact_sales.join(
        dim_customers_renamed,
        fact_sales.sale_customer_id == dim_customers_renamed.customer_id_dim,
        "left"
    )
    top_customers = sales_with_customers \
        .withColumn("customer_name", concat_ws(" ", col("customer_first_name"), col("customer_last_name"))) \
        .groupBy("sale_customer_id", "customer_name") \
        .agg(
            sum("sale_total_price").alias("total_spent"),
            count(fact_sales.sale_customer_id).alias("total_orders"),
            avg("sale_total_price").alias("avg_order_value")
        ) \
        .orderBy(desc("total_spent")) \
        .limit(10)
    country_distribution = sales_with_customers \
        .groupBy("customer_country") \
        .agg(
            countDistinct(fact_sales.sale_customer_id).alias("customer_count"),
            sum("sale_total_price").alias("total_revenue")
        )
    return top_customers, country_distribution

def create_time_sales_report(fact_sales, dim_dates):
    dim_dates_renamed = dim_dates.select(
        col("date_id").alias("date_id_dim"),
        col("sale_date"),
        col("year"),
        col("month")
    )
    sales_with_dates = fact_sales.join(
        dim_dates_renamed,
        fact_sales.date_id == dim_dates_renamed.date_id_dim,
        "left"
    )
    time_trends = sales_with_dates \
        .groupBy("year", "month") \
        .agg(
            sum("sale_total_price").alias("monthly_revenue"),
            avg("sale_total_price").alias("avg_order_value"),
            count("date_id").alias("order_count")
        ) \
        .orderBy("year", "month")
    return time_trends

def create_store_sales_report(fact_sales, dim_stores):
    dim_stores_renamed = dim_stores.select(
        col("store_name").alias("store_name_dim"),
        col("store_city"),
        col("store_country")
    )
    sales_with_stores = fact_sales.join(
        dim_stores_renamed,
        fact_sales.store_name == dim_stores_renamed.store_name_dim,
        "left"
    )
    top_stores = sales_with_stores \
        .groupBy("store_name") \
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            count("store_name").alias("order_count"),
            avg("sale_total_price").alias("avg_order_value")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    store_distribution = sales_with_stores \
        .groupBy("store_country", "store_city") \
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            countDistinct("store_name").alias("store_count")
        )
    return top_stores, store_distribution

def create_product_quality_report(fact_sales, dim_products):
    dim_products_renamed = dim_products.select(
        col("sale_product_id").alias("product_id_dim"),
        col("product_name")
    )
    sales_with_products = fact_sales.join(
        dim_products_renamed,
        fact_sales.sale_product_id == dim_products_renamed.product_id_dim,
        "left"
    )
    product_ratings = sales_with_products \
        .groupBy("sale_product_id", "product_name") \
        .agg(
            avg("product_rating").alias("avg_rating"),
            sum("product_reviews").alias("total_reviews"),
            sum("sale_quantity").alias("total_quantity")
        ) \
        .orderBy(desc("avg_rating"))
    return product_ratings

def write_to_clickhouse(df, table_name, mode="append"):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "123456") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode(mode) \
        .save()

def main():
    spark = create_spark_session()
    # Читаем данные из PostgreSQL
    fact_sales = read_from_postgres(spark, "fact_sales")
    dim_products = read_from_postgres(spark, "dim_products")
    dim_customers = read_from_postgres(spark, "dim_customers")
    dim_stores = read_from_postgres(spark, "dim_stores")
    dim_dates = read_from_postgres(spark, "dim_dates")
    # Создаем отчеты
    top_products, category_revenue = create_product_sales_report(fact_sales, dim_products)
    top_customers, country_distribution = create_customer_sales_report(fact_sales, dim_customers)
    time_trends = create_time_sales_report(fact_sales, dim_dates)
    top_stores, store_distribution = create_store_sales_report(fact_sales, dim_stores)
    product_ratings = create_product_quality_report(fact_sales, dim_products)
    # Записываем отчеты в ClickHouse
    write_to_clickhouse(top_products, "top_products", mode="append")
    write_to_clickhouse(category_revenue, "category_revenue", mode="append")
    write_to_clickhouse(top_customers, "top_customers", mode="append")
    write_to_clickhouse(country_distribution, "country_distribution", mode="append")
    write_to_clickhouse(time_trends, "time_trends", mode="append")
    write_to_clickhouse(top_stores, "top_stores", mode="append")
    write_to_clickhouse(store_distribution, "store_distribution", mode="append")
    write_to_clickhouse(product_ratings, "product_ratings", mode="append")
    spark.stop()

if __name__ == "__main__":
    main()
