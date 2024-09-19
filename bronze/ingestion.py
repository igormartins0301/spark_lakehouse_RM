#%%
import sys
import os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from libs.ingestor import Ingestor

ing = Ingestor(catalog_load='raw', catalog_write='bronze', schema='RickMorty', tablename='characters')

df = ing.load('json')
ing.save(df=df,data_format='parquet', mode='overwrite')

