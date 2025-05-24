from pyspark.sql import SparkSession
import os

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("silver_to_gold_iceberg")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/gold/icebergTables")
    .getOrCreate()
)

print("Spark is running")

# Tạo database gold trong catalog lakehouse
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Lấy min, max date để dùng cho dim_dates
min_max = spark.sql("""
SELECT 
  MIN(order_date) AS min_date, 
  MAX(due_date) AS max_date
FROM iceberg.`s3a://warehouse/silver/icebergTables/crm/crm_sales_details`
""").collect()[0]

min_date = min_max.min_date.strftime('%Y-%m-%d')
max_date = min_max.max_date.strftime('%Y-%m-%d')

# Tạo bảng dim_customers
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customers (
    customer_id STRING,
    customer_crm_id STRING,
    firstname STRING,
    lastname STRING,
    customer_country STRING,
    marital_status STRING,
    gender STRING,
    birthday DATE,
    join_crm_date DATE,
    customer_key INT
)
USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.dim_customers
SELECT
    ci.customer_id,
    ci.customer_crm_id,
    ci.firstname,
    ci.lastname,
    la.customer_country,
    ci.marital_status,
    CASE 
      WHEN ci.gender != 'n/a' THEN ci.gender 
      ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birthday,
    ci.join_crm_date,
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key
FROM iceberg.`s3a://warehouse/silver/icebergTables/crm/crm_customers` ci
LEFT JOIN iceberg.`s3a://warehouse/silver/icebergTables/erp/erp_customer_demographic` ca ON ci.customer_id = ca.customer_id
LEFT JOIN iceberg.`s3a://warehouse/silver/icebergTables/erp/erp_customer_location` la ON ci.customer_id = la.customer_id
""")

# Tạo bảng dim_products
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_products (
    product_id STRING,
    product_crm_id STRING,
    product_name STRING,
    category_id STRING,
    category_name STRING,
    subcategory_name STRING,
    maintenance_flag STRING,
    product_cost DOUBLE,
    product_line STRING,
    start_date DATE,
    end_date DATE,
    product_key INT
)
USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.dim_products
SELECT
    pro.product_id,
    pro.product_crm_id,
    pro.product_name,
    pro.category_id,
    cate.category_name,
    cate.subcategory_name,
    cate.maintenance_flag,
    pro.product_cost,
    pro.product_line,
    pro.start_date,
    pro.end_date,
    ROW_NUMBER() OVER (ORDER BY pro.product_id, pro.start_date, pro.end_date) AS product_key
FROM iceberg.`s3a://warehouse/silver/icebergTables/crm/crm_products` pro
LEFT JOIN iceberg.`s3a://warehouse/silver/icebergTables/erp/erp_categories` cate ON pro.category_id = cate.category_id
""")

# Tạo bảng dim_dates
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_dates (
    date_id DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week INT,
    day_name STRING,
    month_name STRING,
    is_weekend BOOLEAN
)
USING iceberg
""")

spark.sql(f"""
INSERT OVERWRITE lakehouse.gold.dim_dates
SELECT
    date_id,
    DAY(date_id) AS day,
    MONTH(date_id) AS month,
    YEAR(date_id) AS year,
    QUARTER(date_id) AS quarter,
    DAYOFWEEK(date_id) AS day_of_week,
    DATE_FORMAT(date_id, 'EEEE') AS day_name,
    DATE_FORMAT(date_id, 'MMMM') AS month_name,
    CASE WHEN DAYOFWEEK(date_id) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT EXPLODE(SEQUENCE(TO_DATE('{min_date}'), TO_DATE(DATE_ADD('{max_date}', 365)), INTERVAL 1 DAY)) AS date_id
)
""")

# Tạo bảng fact_sales
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_sales (
    order_id STRING,
    product_key INT,
    customer_key INT,
    order_date DATE,
    ship_date DATE,
    due_date DATE,
    price DOUBLE,
    quantity INT,
    total_amount DOUBLE
)
USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.fact_sales
SELECT
    sd.order_id,
    pro.product_key,
    cust.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.price,
    sd.quantity,
    sd.total_amount
FROM iceberg.`s3a://warehouse/silver/icebergTables/crm/crm_sales_details` sd
LEFT JOIN lakehouse.gold.dim_products pro ON sd.product_id = pro.product_id
LEFT JOIN lakehouse.gold.dim_customers cust ON sd.customer_crm_id = cust.customer_crm_id
""")

# Kiểm tra dữ liệu fact_sales
spark.sql("SELECT * FROM lakehouse.gold.fact_sales LIMIT 10").show()
