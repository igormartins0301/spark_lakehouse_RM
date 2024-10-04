# %%
import os
import sys
import json
parent_directory = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.append(parent_directory)


from libs.ingestor import GoldIngestor

with open('workflow.json', 'r') as file:
    configurations = json.load(file)

for config in configurations:
    print(f'Fazendo a atualização da tabela: {config["tablename_save"]}')
    tablename_load = config['tablename_load']
    tablename_save = config['tablename_save']
    schema = config['schema']
    date_field = config['date_field']
    new_table = config['new_table']

    gold_ingestor = GoldIngestor(
        schema=schema,
        tablename_load=tablename_load,
        tablename_save=tablename_save,
        date_field=date_field,
    )


    gold_ingestor.ingest_to_gold(sql_file_path=f'{tablename_save}.sql', new_table=new_table)
print('Atualizações finalizadas')