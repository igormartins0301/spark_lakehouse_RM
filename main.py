#%%
#API-EXTRACT_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=1, 
                          final_id=10, 
                          bucket_name='raw', 
                          folder_name='RickMorty', 
                          tablename='characters',
                          url='https://rickandmortyapi.com/api/character/')
extractor.run()

#%%
#READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(schema='RickMorty', tablename='characters')
df = ing.load('delta', catalog='bronze')
df.show()
df.count()
df.columns

