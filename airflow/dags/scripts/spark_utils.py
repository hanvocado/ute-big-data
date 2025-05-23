import os

from pyspark.sql import SparkSession


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_ENDPOINT = "http://minio:9000" 
AWS_BUCKET_NAME = "warehouse" 

def spark_session():
    """
    Create and configure a Spark session for interacting with the MinIO S3 storage
    and Hive Metastore, with Iceberg support.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = (
        SparkSession.builder.appName("ERP/CRM ETL") 
        .master("spark://spark-master:7077")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")

        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Iceberg configurations
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Configure the default Spark catalog to use Iceberg Hive catalog
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083")
        # Spark SQL warehouse directory pointing to the lakehouse bucket on S3A
        .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{AWS_BUCKET_NAME}/") # Set warehouse root to the bucket root
        # Jars required for S3A, Hive, and Iceberg
        # NOTE: Replace with actual JAR filenames and paths based on your Spark distribution
        .config(
            "spark.jars.packages",
            "/opt/spark/jars/hadoop-aws-3.3.1.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.150.jar"
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    return spark
