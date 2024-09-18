#%%
#INGESTION_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=7, final_id=9, bucket_name='raw', folder_name='RickMorty', url='https://rickandmortyapi.com/api/character/')
extractor.run()


#%%
from libs.ingestor import Ingestor

ing = Ingestor(catalog_load='raw', catalog_write='bronze', schema='RickMorty')

df = ing.load('json')

ing.save(df, 'characters','delta',mode ='overwrite')