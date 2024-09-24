#%%
#API-EXTRACT_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(initial_id=15, 
                          final_id=30, 
                          bucket_name='raw', 
                          folder_name='rickmorty', 
                          tablename='episodes',
                          url='https://rickandmortyapi.com/api/location/')
extractor.run(full_load=False)