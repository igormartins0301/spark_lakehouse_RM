import json
import os
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv


class DataExtractor:
    def __init__(
        self, initial_id, final_id, bucket_name, folder_name, tablename
    ):
        self.initial_id = initial_id
        self.final_id = final_id
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.tablename = tablename
        self.s3_client = self.configure_s3_client()
        self.url = f'https://rickandmortyapi.com/api/{self.tablename}/'
        load_dotenv()

    def configure_s3_client(self):
        """Configura o cliente S3 para o MinIO usando variáveis de ambiente."""
        return boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        )

    def fetch_data(self, full_load=False):
        """Obtém dados da API Rick and Morty."""
        if full_load:
            url = self.url
        else:
            id_range = ','.join(
                str(i) for i in range(self.initial_id, self.final_id + 1)
            )
            url = f'{self.url}{id_range}'

        print(url)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Falha ao obter dados: {response.status_code}')

    def add_updated_at(self, data):
        """Adiciona a coluna 'updated_at' com o timestamp atual a cada dicionário na lista."""
        current_timestamp = (
            datetime.now().isoformat()
        )  # Formato ISO para o timestamp

        # Verifica se 'data' é uma lista
        if isinstance(data, list):
            for item in data:
                if isinstance(
                    item, dict
                ):  # Verifica se cada item é um dicionário
                    item['updated_at'] = (
                        current_timestamp  # Adiciona a coluna 'updated_at'
                    )
                else:
                    raise ValueError(
                        'Um dos itens na lista não é um dicionário.'
                    )
        else:
            raise ValueError('Os dados não estão no formato esperado (lista).')

        return data

    def generate_object_key(self):
        """Gera um nome de arquivo baseado na data atual e um número sequencial."""
        today = datetime.now().strftime('%Y-%m-%d')

        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=f'{self.folder_name}/{self.tablename}/{today}_',
        )

        if 'Contents' in response:
            existing_files = [obj['Key'] for obj in response['Contents']]
            existing_numbers = [
                int(key.split('_')[-1].split('.')[0])
                for key in existing_files
                if key.startswith(
                    f'{self.folder_name}/{self.tablename}/{today}_'
                )
            ]
            next_number = max(existing_numbers) + 1 if existing_numbers else 1
        else:
            next_number = 1

        return f'{self.folder_name}/{self.tablename}/{today}_{next_number:03}.json'

    def save_data_to_minio(self, data):
        """Salva os dados JSON no MinIO."""
        object_key = self.generate_object_key()  # Gera o nome do arquivo

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=json.dumps(data),
            ContentType='application/json',
        )

        print(
            f"Dados JSON carregados com sucesso no MinIO como '{object_key}'."
        )

    def run(self, full_load):
        """Executa todo o processo de extração e armazenamento."""
        try:
            dados = self.fetch_data(full_load=full_load)
            dados_with_timestamp = self.add_updated_at(
                dados
            )  # Adiciona a coluna 'updated_at'
            self.save_data_to_minio(dados_with_timestamp)
        except Exception as e:
            print(self.url)
            print(f'Erro: {e}')
