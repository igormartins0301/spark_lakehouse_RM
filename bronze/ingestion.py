# %%
import os
import sys
import json

parent_directory = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.append(parent_directory)


from libs.ingestor import Ingestor, IngestorCDC

with open('workflow.json', 'r') as file:
    configurations = json.load(file)

for config in configurations:
    full_load = config['full_load']
    tablename_load = config['tablename_load']
    tablename_save = config['tablename_save']
    schema = config['schema']
    id_field = config['id_field']
    timestamp_field = config['timestamp_field']


    if full_load:
        ing = Ingestor(
            schema=schema,
            tablename_load=tablename_load,
            tablename_save=tablename_save,
        )

        df = ing.load(data_format='json', catalog='raw')
        ing.save(df=df, data_format='delta', mode='overwrite', catalog='bronze')

    else:
        ing = IngestorCDC(
            schema=schema,
            tablename_load=tablename_load,
            tablename_save=tablename_save,
            id_field=id_field,
            timestamp_field=timestamp_field,
        )

        static_df = ing.load(data_format='json', catalog='raw')
        inferred_schema = static_df.schema
        rawdf = ing.load_streaming(
            'json', catalog='raw', infered_schema=inferred_schema
        )
        ing.save_streaming(rawdf)
