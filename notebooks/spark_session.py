from pyspark.sql import SparkSession

BASE_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",               # accès s3a:// (MinIO)
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

def get_spark(app_name, extra_packages=None, extra_conf=None):
    packages = BASE_PACKAGES + (extra_packages or [])
    b = (SparkSession.builder
         .appName(app_name)
         .master("spark://spark-master:7077")
         .config("spark.driver.host", "jupyter")
         .config("spark.driver.bindAddress", "0.0.0.0")
         .config("spark.executor.memory", "512m")
         .config("spark.sql.shuffle.partitions", "8")  
         .config("spark.jars.packages", ",".join(packages))
         .config("spark.jars.repositories", "https://repos.spark-packages.org")
         # MinIO via S3A
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    for k, v in (extra_conf or {}).items():
        b = b.config(k, v)
    return b.getOrCreate()