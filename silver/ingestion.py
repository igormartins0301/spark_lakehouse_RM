#%%
import sys
import os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)


from libs.ingestor import Ingestor, IngestorCDC, SilverIngestor

schema = 'rickmorty'
tablename = 'episode'
merge_condition = 't.idEpisodio = s.idEpisodio' #Silver incremental merge


ing = SilverIngestor(schema=schema, tablename=tablename)
ing.set_bronze_source(schema, tablename)
ing.ingest_to_silver('episode.sql')

