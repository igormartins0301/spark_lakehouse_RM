import os
from dotenv import load_dotenv
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

load_dotenv()

class SparkReadWrite:
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema
        
        self.spark = SparkSession.builder \
            .appName("job-1-spark") \
            .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
            .config("fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT')) \
            .config("fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY')) \
            .config("fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY')) \
            .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("fs.s3a.path.style.access", "True") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_files(self, data_format: str) -> DataFrame:
        df = self.spark.read.format(data_format) \
            .option("header", "True") \
            .option("inferSchema", "True") \
            .load(f"s3a://{self.catalog}/{self.schema}/*.{data_format}")
        return df