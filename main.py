#%%
#INGESTION_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=3, final_id=4, bucket_name='raw', folder_name='RickMorty', url='https://rickandmortyapi.com/api/character/')
extractor.run()

#%%
# READ_EXAMPLE
from libs.ingestor import SparkReadWrite

spark = SparkReadWrite(catalog= 'raw', schema= 'RickMorty')

df = spark.read_files(data_format='json')

df.show()