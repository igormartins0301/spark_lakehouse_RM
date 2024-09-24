#%%
#API-EXTRACT_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=7, 
                          final_id=20, 
                          bucket_name='raw', 
                          folder_name='RickMorty', 
                          tablename='episodes',
                          url='https://rickandmortyapi.com/api/location/')
extractor.run(full_load=False)