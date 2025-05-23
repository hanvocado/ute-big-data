import sys
import os
import argparse

# Add the directory containing spark_utils.py to the Python path
# This assumes spark_utils.py is in the same directory or accessible
# Adjust the path if spark_utils.py is located elsewhere relative to this script
current_dir = os.path.dirname(os.path.abspath(__file__))
spark_utils_path = os.path.join(current_dir, 'spark_utils.py')
# Check if spark_utils.py exists before adding to path
if os.path.exists(spark_utils_path):
     sys.path.append(current_dir)
else:
     print(f"Warning: spark_utils.py not found at {spark_utils_path}. Ensure it's in the correct location.")
     # Depending on your setup, you might need a different approach to import spark_utils


from spark_utils import spark_session
# We might not need specific functions imported if using pure SQL strings,
# but keeping common ones might be useful for helper logic.
# Removing the problematic imports like 'cast' and 'nullif'
from pyspark.sql.functions import col # Keep col if needed for read/write options


def transform_bronze_to_silver(year, month):
    """
    Loads data from the bronze layer (Minio S3A), performs transformations
    using Spark SQL, and saves the results to the
    silver layer as Iceberg file-based datasets, partitioned by year/month in path.

    Args:
        year (str): The year partition (YYYY).
        month (str): The month partition (MM).
    """
    spark = spark_session()

    # Define S3A paths for bronze data.
    # Assuming the files are still CSVs in the specified bronze directories.
    # We'll read these into temporary views to query with SQL.
    bronze_path = "s3a://warehouse/bronze"
    crm_cust_info_path = f"{bronze_path}/crm/cust_info.csv"
    crm_prd_info_path = f"{bronze_path}/crm/prd_info.csv"
    crm_sales_details_path = f"{bronze_path}/crm/sales_details.csv"
    erp_loc_a101_path = f"{bronze_path}/erp/LOC_A101.csv"
    erp_cust_az12_path = f"{bronze_path}/erp/CUST_AZ12.csv"
    erp_px_cat_g1v2_path = f"{bronze_path}/erp/PX_CAT_G1V2.csv"

    # Define target S3A paths for silver datasets using year and month args
    silver_base_path = "s3a://warehouse/silver"
    # Format month with leading zero - convert month to int first for formatting
    silver_partition_path = f"{silver_base_path}/{year}/{int(month):02d}"

    silver_customers_path = f"{silver_partition_path}/crm_customers"
    silver_products_path = f"{silver_partition_path}/crm_products"
    silver_sales_details_path = f"{silver_partition_path}/crm_sales_details"
    silver_erp_loc_path = f"{silver_partition_path}/erp_customer_location"
    silver_erp_demo_path = f"{silver_partition_path}/erp_customer_demographic"
    silver_erp_categories_path = f"{silver_partition_path}/erp_categories"


    print(f"Using Spark Catalog: {spark.conf.get('spark.sql.catalog.spark_catalog')}")
    print(f"Using Hive Metastore URI: {spark.conf.get('spark.sql.catalog.spark_catalog.uri')}")
    print(f"Using Warehouse Dir: {spark.conf.get('spark.sql.warehouse.dir')}")
    print(f"Saving data to Silver partition path: {silver_partition_path}")

    print("Starting Bronze to Silver transformation using Spark SQL...")

    try:

        spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.silver")

        # Read Bronze CSVs and create temporary views
        print("Reading Bronze data and creating temporary views...")
        spark.read.csv(crm_cust_info_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_crm_cust_info")
        spark.read.csv(crm_prd_info_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_crm_prd_info")
        spark.read.csv(crm_sales_details_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_crm_sales_details")
        spark.read.csv(erp_loc_a101_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_erp_loc_a101")
        spark.read.csv(erp_cust_az12_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_erp_cust_az12")
        spark.read.csv(erp_px_cat_g1v2_path, header=True, inferSchema=True).createOrReplaceTempView("bronze_erp_px_cat_g1v2")
        print("Temporary views created.")

        # --- Transform and Load crm_customers ---
        print("Processing crm_customers...")
        spark.sql("""
            SELECT
                cst_id AS customer_crm_id,
                cst_key AS customer_id,
                TRIM(cst_firstname) AS firstname,
                TRIM(cst_lastname) AS lastname,
                CASE
                    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                END AS marital_status,
                CASE
                    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    ELSE 'n/a'
                END AS gender,
                TO_DATE(cst_create_date) AS join_crm_date
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                FROM bronze_crm_cust_info
                WHERE cst_id IS NOT NULL
            ) t
            WHERE flag_last = 1
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_customers_path) \
            .saveAsTable("spark_catalog.silver.crm_customers")
        print(f"Successfully loaded data into spark_catalog.silver.crm_customers at {silver_customers_path}")

        # --- Transform and Load crm_products ---
        print("Processing crm_products...")
        spark.sql("""
            SELECT
                prd_id AS product_crm_id,
                SUBSTRING(prd_key, 7) AS product_id,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
                prd_nm AS product_name,
                COALESCE(prd_cost, 0) AS product_cost,
                CASE
                    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                    ELSE 'n/a'
                END AS product_line,
                TO_DATE(prd_start_dt) AS start_date,
                CAST(
                    LEAD(TO_DATE(prd_start_dt), 1, NULL) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY
                    AS DATE
                ) AS end_date
            FROM bronze_crm_prd_info
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_products_path) \
            .saveAsTable("spark_catalog.silver.crm_products")
        print(f"Successfully loaded data into spark_catalog.silver.crm_products at {silver_products_path}")

        # --- Transform and Load crm_sales_details ---
        print("Processing crm_sales_details...")
        spark.sql("""
            SELECT
                sls_ord_num AS order_id,
                sls_prd_key AS product_id,
                sls_cust_id AS customer_crm_id,
                CASE
                    WHEN sls_order_dt IS NULL OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
                    ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
                END AS order_date,
                CASE
                    WHEN sls_ship_dt IS NULL OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
                    ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
                END AS ship_date,
                CASE
                    WHEN sls_due_dt IS NULL OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
                    ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
                END AS due_date,
                CASE
                    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                        THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END AS total_amount,
                sls_quantity AS quantity,
                CASE
                    WHEN sls_price IS NULL OR sls_price <= 0
                        THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END AS price
            FROM bronze_crm_sales_details
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_sales_details_path) \
            .saveAsTable("spark_catalog.silver.crm_sales_details")
        print(f"Successfully loaded data into spark_catalog.silver.crm_sales_details at {silver_sales_details_path}")

        # --- Transform and Load erp_customer_location ---
        print("Processing erp_customer_location...")
        spark.sql("""
            SELECT
                REPLACE(cid, '-', '') AS customer_id,
                CASE
                    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                    ELSE TRIM(cntry)
                END AS customer_country
            FROM bronze_erp_loc_a101
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_erp_loc_path) \
            .saveAsTable("spark_catalog.silver.erp_customer_location")
        print(f"Successfully loaded data into spark_catalog.silver.erp_customer_location at {silver_erp_loc_path}")

        # --- Transform and Load erp_customer_demographic ---
        print("Processing erp_customer_demographic...")
        spark.sql("""
            SELECT
                CASE
                    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
                    ELSE cid
                END AS customer_id,
                CASE
                    WHEN bdate > CURRENT_DATE() THEN NULL
                    ELSE TO_DATE(bdate)
                END AS birthday,
                CASE
                    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                    ELSE 'n/a'
                END AS gender
            FROM bronze_erp_cust_az12
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_erp_demo_path) \
            .saveAsTable("spark_catalog.silver.erp_customer_demographic")
        print(f"Successfully loaded data into spark_catalog.silver.erp_customer_demographic at {silver_erp_demo_path}")

        # --- Transform and Load erp_categories ---
        print("Processing erp_categories...")
        spark.sql("""
            SELECT
                id AS category_id,
                cat AS category_name,
                subcat AS subcategory_name,
                CASE
                    WHEN UPPER(TRIM(maintenance)) = 'YES' THEN TRUE
                    ELSE FALSE
                END AS maintenance_flag
            FROM bronze_erp_px_cat_g1v2
        """).write \
            .mode("overwrite") \
            .format("iceberg") \
            .option("path", silver_erp_categories_path) \
            .saveAsTable("spark_catalog.silver.erp_categories")
        print(f"Successfully loaded data into spark_catalog.silver.erp_categories at {silver_erp_categories_path}")

        print("Bronze to Silver transformation completed using Spark SQL.")

    except Exception as e:
        print(f"An error occurred during Spark SQL transformation: {e}")
        raise # Re-raise the exception to fail the task

    finally:
        # Ensure Spark session is stopped even if an error occurs
        if 'spark' in locals() and spark:
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    # Basic check for environment variables before starting Spark
    # spark_utils also checks, but an early check is good.
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        print("Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set.")
        sys.exit(1) # Exit with an error code

    # Parse year and month from command line arguments
    parser = argparse.ArgumentParser(description="Spark script to transform Bronze to Silver using Spark SQL.")
    parser.add_argument("--year", type=str, required=True, help="Year partition (YYYY)")
    parser.add_argument("--month", type=str, required=True, help="Month partition (MM)")
    args = parser.parse_args()

    transform_bronze_to_silver(args.year, args.month)
