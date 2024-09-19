#%%
#INGESTION_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=1, 
                          final_id=10, 
                          bucket_name='raw', 
                          folder_name='RickMorty', 
                          tablename='characters',
                          url='https://rickandmortyapi.com/api/character/')
extractor.run()


#%%
from libs.ingestor import Ingestor

ing = Ingestor(catalog_load='bronze', catalog_write='bronze', schema='RickMorty', tablename='characters')

df = ing.load('parquet')

#%%
df.show(5)