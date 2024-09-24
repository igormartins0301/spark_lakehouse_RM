#%%
import sys
import os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)
from libs.ingestor import Ingestor, IngestorCDC
#%%
#FULL LOAD EXAMPLE
tablename = 'episodes'
schema='rickmorty'

ing = Ingestor(schema=schema, tablename=tablename)

df = ing.load(data_format='json', catalog='raw')
ing.save(df=df,data_format='delta', mode='overwrite', catalog='bronze')
#%%
########################################
#CDC INCREMENTAL LOAD EXAMPLE
tablename = 'episodes'
schema='rickmorty'
id_field = 'id'
timestamp_field = 'updated_at'

ing = IngestorCDC(schema=schema, 
                  tablename=tablename,
                  id_field=id_field,
                  timestamp_field=timestamp_field)

static_df = ing.load(data_format='json', catalog='raw')
inferred_schema = static_df.schema
rawdf = ing.load_streaming('json', catalog='raw', infered_schema=inferred_schema)
ing.save_streaming(rawdf)
