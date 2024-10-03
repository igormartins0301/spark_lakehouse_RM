# %%
# API-EXTRACT_EXAMPLE
from libs.api_extractor import DataExtractor

extractor = DataExtractor(
    initial_id=20,
    final_id=60,
    bucket_name='raw',
    folder_name='rickmorty',
    tablename='episode',
)
extractor.run(full_load=False)
