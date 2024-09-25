import os
from dotenv import load_dotenv
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, functions as F
from delta import *
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from typing import Optional, List, Dict, Any

load_dotenv()

class Ingestor:
    def __init__(self, schema: str, tablename:str):
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

            
        #self.spark.sparkContext.setLogLevel("DEBUG")

    def load(self, data_format: str, catalog:str) -> DataFrame:
        if data_format =='delta':
            df = self.spark.read.format("delta") \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .load(f"s3a://{catalog}/{self.schema}/{self.tablename}")
            return df
        else:
            df = self.spark.read.format(data_format) \
                .option("header", "True") \
                .option("inferSchema", "True") \
                .load(f"s3a://{catalog}/{self.schema}/{self.tablename}/*.{data_format}")
            return df
        
    def save(self, df: DataFrame, data_format:str, catalog:str,  mode: str = "overwrite"):
        (df.write
         .format(data_format)
         .mode(mode)
         .save(f"s3a://{catalog}/{self.schema}/{self.tablename}"))
        return True
    
    def execute_query(self, query):
        self.spark.sql(query)
        return True



class IngestorCDC(Ingestor):
    def __init__(self, schema: str, tablename: str, id_field: str, timestamp_field: str):
        super().__init__(schema, tablename)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self, catalog='bronze'):
        """Configura a tabela Delta para operações de upsert."""
        tablename = f"{self.schema}.{self.tablename}"
        self.deltatable = DeltaTable.forPath(self.spark, f"s3a://{catalog}/{self.schema}/{self.tablename}")

    def upsert(self, df: DataFrame):
        """Realiza o upsert na tabela Delta."""
        df.createOrReplaceGlobalTempView(f"view_{self.tablename}")

        query = f'''
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) AS rn
                    FROM global_temp.view_{self.tablename}
                ) AS subquery
                WHERE rn = 1
            '''
        
        df_cdc = self.spark.sql(query)

        # Realiza o merge na tabela Delta
        (self.deltatable.alias("b")
             .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}") 
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())

    def load(self, data_format: str, catalog: str) -> DataFrame:
        return super().load(data_format, catalog)

    def save(self, df: DataFrame, data_format: str, catalog: str, mode: str = "overwrite"):
        return super().save(df, data_format, catalog, mode)
    
    def load_streaming(self, data_format: str, catalog: str, infered_schema) -> DataFrame:
        """Carrega os dados como um DataFrame de streaming."""
        return (self.spark
                    .readStream
                    .schema(infered_schema)
                    .format(data_format)
                    .option("header", "true")
                    .option("maxFilesPerTrigger", 1)
                    .load(f"s3a://{catalog}/{self.schema}/{self.tablename}"))
    
    def save_streaming(self, df: DataFrame):
        (df.writeStream
            .option("checkpointLocation", f"s3a://checkpoints/{self.schema}/{self.tablename}_checkpoint/")
            .foreachBatch(lambda df, batchID: self.upsert(df))
            .trigger(availableNow=True)
            .start())


    def execute_query(self, query):
        return super().execute_query(query)
    

class SilverIngestor(Ingestor):
    def __init__(self, schema: str, tablename: str):
        super().__init__(schema, tablename)
        self.bronze_schema: Optional[str] = None
        self.bronze_table: Optional[str] = None
    
    def set_bronze_source(self, bronze_schema: str, bronze_table: str) -> None:
        """Define a tabela fonte na camada Bronze."""
        self.bronze_schema = bronze_schema
        self.bronze_table = bronze_table
    
    def read_sql_file(self, sql_file_path: str) -> str:
        """Lê um arquivo SQL e retorna o conteúdo como uma string."""
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        return sql_query
    
    def process_bronze_to_silver(self, sql_query: str) -> DataFrame:
        """
        Processa dados da Bronze para Silver usando uma consulta SQL.
        
        :param sql_query: Consulta SQL para transformar os dados
        :return: DataFrame com os dados processados
        """
        if not self.bronze_schema or not self.bronze_table:
            raise ValueError("Bronze source not set. Call set_bronze_source() first.")

        # Carrega a tabela da camada Bronze
        bronze_df = self.load("delta", "bronze")
        bronze_df.createOrReplaceTempView(f"{self.bronze_schema}_{self.bronze_table}")

        # Executa a consulta SQL nas tabelas bronze
        result_df = self.spark.sql(sql_query)
        return result_df
    
    def ingest_to_silver(self, sql_file_path: str, merge_condition: Optional[str] = None) -> None:
        """
        Ingere os dados processados da camada Bronze para a Silver usando um arquivo SQL.
        
        :param sql_file_path: Caminho do arquivo SQL
        :param merge_condition: Condição para merge (se aplicável)
        """
        # Passo 1: Ler o arquivo SQL
        sql_query = self.read_sql_file(sql_file_path)

        # Extrair o nome do arquivo SQL sem a extensão .sql
        table_name_from_file = os.path.basename(sql_file_path).replace('.sql', '')

        # Passo 2: Processar a transformação do Bronze para o Silver
        transformed_df = self.process_bronze_to_silver(sql_query)

        # Passo 3: Salvar os dados na Silver
        silver_table_path = f"s3a://silver/{self.schema}/{table_name_from_file}"

        # Passo 3: Salvar os dados na Silver
        if merge_condition:
            silver_table = DeltaTable.forPath(self.spark, silver_table_path)
            
            (silver_table.alias("t")
             .merge(transformed_df.alias("s"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        else:
            (transformed_df.write
               .format('delta')
               .mode('overwrite')
               .save(silver_table_path))