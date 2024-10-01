#%%
import sys
import os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)


from libs.ingestor import Ingestor, IngestorCDC, SilverIngestor

schema = 'rickmorty'
tablename_load = 'location'
tablename_save = 'location_residents'
merge_condition = 't.idEpisodio = s.idEpisodio' #Silver incremental merge


ing = SilverIngestor(schema=schema, tablename_load=tablename_load, tablename_save=tablename_save)
# ing.set_bronze_source(bronze_schema='rickmorty', bronze_table='episode')
ing.ingest_to_silver(f'{tablename_save}.sql')

