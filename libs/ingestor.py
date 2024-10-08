import os
from typing import Optional

from delta import *
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

load_dotenv()


class Ingestor:
    def __init__(self, schema: str, tablename_load: str, tablename_save: str):
        self.tablename_load = tablename_load
        self.tablename_save = tablename_save
        self.schema = schema

        self.builder = (
            SparkSession.builder.appName('job-1-spark')
            .config('fs.s3a.endpoint', os.getenv('MINIO_ENDPOINT'))
            .config('fs.s3a.access.key', os.getenv('MINIO_ACCESS_KEY'))
            .config('fs.s3a.secret.key', os.getenv('MINIO_SECRET_KEY'))
            .config('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('fs.s3a.path.style.access', 'True')
            .config(
                'spark.sql.extensions',
                'io.delta.sql.DeltaSparkSessionExtension',
            )
            .config(
                'spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            )
        )
        self.spark = configure_spark_with_delta_pip(self.builder).getOrCreate()

    def load(self, data_format: str, catalog: str) -> DataFrame:
        if data_format == 'delta':
            df = (
                self.spark.read.format('delta')
                .option('header', 'True')
                .option('inferSchema', 'True')
                .load(f's3a://{catalog}/{self.schema}/{self.tablename_load}')
            )
            return df
        else:
            df = (
                self.spark.read.format(data_format)
                .option('header', 'True')
                .option('inferSchema', 'True')
                .load(
                    f's3a://{catalog}/{self.schema}/{self.tablename_load}/*.{data_format}'
                )
            )
            return df

    def save(
        self,
        df: DataFrame,
        data_format: str,
        catalog: str,
        mode: str = 'overwrite',
    ):
        (
            df.write.format(data_format)
            .mode(mode)
            .save(f's3a://{catalog}/{self.schema}/{self.tablename_save}')
        )
        return True

    def execute_query(self, query):
        self.spark.sql(query)
        return True


class IngestorCDC(Ingestor):
    def __init__(
        self,
        schema: str,
        tablename_load: str,
        tablename_save: str,
        id_field: str,
        timestamp_field: str,
    ):
        super().__init__(schema, tablename_load, tablename_save)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self, catalog='bronze'):
        """Configura a tabela Delta para operações de upsert."""
        tablename = f'{self.schema}.{self.tablename_load}'
        self.deltatable = DeltaTable.forPath(
            self.spark, f's3a://{catalog}/{self.schema}/{self.tablename_load}'
        )

    def upsert(self, df: DataFrame):
        """Realiza o upsert na tabela Delta."""
        df.createOrReplaceGlobalTempView(f'view_{self.tablename_load}')

        query = f"""
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) AS rn
                    FROM global_temp.view_{self.tablename_save}
                ) AS subquery
                WHERE rn = 1
            """

        df_cdc = self.spark.sql(query)

        (
            self.deltatable.alias('b')
            .merge(df_cdc.alias('d'), f'b.{self.id_field} = d.{self.id_field}')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def load(self, data_format: str, catalog: str) -> DataFrame:
        return super().load(data_format, catalog)

    def save(
        self,
        df: DataFrame,
        data_format: str,
        catalog: str,
        mode: str = 'overwrite',
    ):
        return super().save(df, data_format, catalog, mode)

    def load_streaming(
        self, data_format: str, catalog: str, infered_schema
    ) -> DataFrame:
        """Carrega os dados como um DataFrame de streaming."""
        return (
            self.spark.readStream.schema(infered_schema)
            .format(data_format)
            .option('header', 'true')
            .option('maxFilesPerTrigger', 1)
            .load(f's3a://{catalog}/{self.schema}/{self.tablename_load}')
        )

    def save_streaming(self, df: DataFrame):
        (
            df.writeStream.option(
                'checkpointLocation',
                f's3a://checkpoints/{self.schema}/{self.tablename_save}_checkpoint/',
            )
            .foreachBatch(lambda df, batchID: self.upsert(df))
            .trigger(availableNow=True)
            .start()
        )

    def execute_query(self, query):
        return super().execute_query(query)


class SilverIngestor(Ingestor):
    def __init__(self, schema: str, tablename_load: str, tablename_save: str):
        super().__init__(schema, tablename_load, tablename_save)
        self.bronze_schema: Optional[str] = None
        self.bronze_table: Optional[str] = None

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
        bronze_df = self.load('delta', catalog='bronze')
        bronze_df.createOrReplaceTempView(
            f'{self.schema}_{self.tablename_load}'
        )
        result_df = self.spark.sql(sql_query)
        return result_df

    def ingest_to_silver(
        self, sql_file_path: str, merge_condition: Optional[str] = None
    ) -> None:
        """
        Ingere os dados processados da camada Bronze para a Silver usando um arquivo SQL.

        :param sql_file_path: Caminho do arquivo SQL
        :param merge_condition: Condição para merge (se aplicável)
        """
        sql_query = self.read_sql_file(sql_file_path)

        table_name_from_file = os.path.basename(sql_file_path).replace(
            '.sql', ''
        )

        transformed_df = self.process_bronze_to_silver(sql_query)
        #transformed_df.groupBy('idLocal', 'idResidente').count().filter('count > 1').show()

        silver_table_path = (
            f's3a://silver/{self.schema}/{table_name_from_file}'
        )
        if merge_condition:
            print('Incremental ingestion...')
            silver_table = DeltaTable.forPath(self.spark, silver_table_path)

            (
                silver_table.alias('t')
                .merge(transformed_df.alias('s'), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            print('Full load...')
            try:
                transformed_df.write.format('delta') \
                                    .mode('overwrite') \
                                    .option("overwriteSchema", "true") \
                                    .save(silver_table_path)
            except Exception as e:
                print(f"Erro ao gravar no MinIO: {e}")


class GoldIngestor(Ingestor):
    def __init__(self, schema: str, tablename_load: str, tablename_save: str, date_field: str):
        super().__init__(schema, tablename_load, tablename_save)  # Chama o construtor da classe Ingestor
        self.date_field = date_field
        self.table = f"{self.schema}.{self.tablename_save}"

    def read_sql_file(self, sql_file_path: str) -> str:
        """Lê um arquivo SQL e retorna o conteúdo como uma string."""
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        return sql_query

    def load_silver(self) -> DataFrame:
        """Carrega os dados da camada Silver e cria uma view temporária."""
        silver_df = self.load('delta', catalog='silver')  # Usa o método da classe base
        silver_df.createOrReplaceTempView(f'{self.schema}_{self.tablename_load}')
        return silver_df

    def process_silver_to_gold(self, sql_query: str) -> DataFrame:
        """Executa a query SQL para processar dados da Silver para Gold."""
        self.load_silver()  # Carrega os dados da Silver
        result_df = self.spark.sql(sql_query)  # Executa a query
        return result_df

    def delete_today_data(self, date: str) -> None:
        """Remove os dados de hoje da tabela Gold."""
        # Carrega a tabela Gold
        gold_df = (
            self.spark.read.format('delta')
            .option('header', 'True')
            .option('inferSchema', 'True')
            .load(f's3a://gold/{self.schema}/{self.tablename_save}')
        )
        
        count_to_delete = gold_df.filter(gold_df[self.date_field] == date).count()

        filtered_df = gold_df.filter(gold_df[self.date_field] != date)

        filtered_df.write.format('delta').mode('overwrite').save(f's3a://gold/{self.schema}/{self.tablename_save}')
        return count_to_delete
        
    def save(self, df: DataFrame, new_table: bool = False):
        """Salva os dados na camada Gold, fazendo append ou criando uma nova tabela."""
        super().save(df, 'delta', catalog='gold', mode='overwrite' if new_table else 'append')

    def ingest_to_gold(self, sql_file_path: str, new_table: bool = False, date: str = None):
        """Ingere dados processados da camada Silver para a Gold de forma incremental ou completa."""

        sql_query = self.read_sql_file(sql_file_path)
        self.delete_today_data(date=date)
        transformed_df = self.process_silver_to_gold(sql_query)
        self.save(transformed_df, new_table=new_table)  
