import os
from dotenv import load_dotenv
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, functions as F
from delta import *
from datetime import datetime

load_dotenv()

class Ingestor:
    def __init__(self, catalog_load: str, catalog_write: str, schema: str, tablename:str):
        self.catalog_load = catalog_load
        self.catalog_write = catalog_write
        self.tablename = tablename
        self.schema = schema

        self.builder = SparkSession.builder \
            .appName("job-1-spark") \
            .config("fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT')) \
            .config("fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY')) \
            .config("fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY')) \
            .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("fs.s3a.path.style.access", "True") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            #.enableHiveSupport()\

        self.spark = configure_spark_with_delta_pip(self.builder).getOrCreate()

            
        self.spark.sparkContext.setLogLevel("DEBUG")

    def load(self, data_format: str) -> DataFrame:
        df = self.spark.read.format(data_format) \
            .option("header", "True") \
            .option("inferSchema", "True") \
            .load(f"s3a://{self.catalog_load}/{self.schema}/{self.tablename}/*.{data_format}")
        return df
        
    def save(self, df: DataFrame, data_format:str, mode: str = "overwrite",):
        current_timestamp = datetime.now()
        df = df.withColumn("last_updated", F.lit(current_timestamp))
        
        (df.write
         .format(data_format)
         .mode(mode)
         .save(f"s3a://{self.catalog_write}/{self.schema}/{self.tablename}"))
        return True
    
    def execute_query(self, query):
        self.spark.sql(query)
        return True
    
