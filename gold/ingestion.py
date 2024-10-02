# %%
import os
import sys

parent_directory = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.append(parent_directory)


from libs.ingestor import GoldIngestor

schema = 'rickmorty'
tablename_load = 'location_residents'
tablename_save = 'count_residents_by_local'
date_field = 'dtAtualizacao'

gold_ingestor = GoldIngestor(
    schema=schema,
    tablename_load=tablename_load,
    tablename_save=tablename_save,
    date_field=date_field,
)

merge_condition = 'g.id = f.id'

gold_ingestor.ingest_to_gold(sql_file_path=f'{tablename_save}.sql')
