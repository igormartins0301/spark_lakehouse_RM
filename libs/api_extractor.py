#%%
import requests
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
import os

class DataExtractor:
    def __init__(self, initial_id, final_id, bucket_name, folder_name,url):
        self.initial_id = initial_id
        self.final_id = final_id
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.s3_client = self.configure_s3_client()
        self.url = url
        load_dotenv()

    def configure_s3_client(self):
        """Configura o cliente S3 para o MinIO usando variáveis de ambiente."""
        return boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
        )

    def fetch_data(self):
        """Obtém dados da API Rick and Morty."""
        url = f'{self.url}{self.initial_id},{self.final_id}'
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Falha ao obter dados: {response.status_code}")

    def generate_object_key(self):
        """Gera um nome de arquivo baseado na data atual e um número sequencial."""
        today = datetime.now().strftime('%Y-%m-%d')
        
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{self.folder_name}/{today}_")
        
        if 'Contents' in response:
            existing_files = [obj['Key'] for obj in response['Contents']]
            existing_numbers = [
                int(key.split('_')[-1].split('.')[0]) for key in existing_files if key.startswith(f"{self.folder_name}/{today}_")
            ]
            next_number = max(existing_numbers) + 1 if existing_numbers else 1
        else:
            next_number = 1
        
        return f"{self.folder_name}/{today}_{next_number:03}.json"

    def save_data_to_minio(self, data):
        """Salva os dados JSON no MinIO."""
        object_key = self.generate_object_key()  # Gera o nome do arquivo
        
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        
        print(f"Dados JSON carregados com sucesso no MinIO como '{object_key}'.")

    def run(self):
        """Executa todo o processo de extração e armazenamento."""
        try:
            dados = self.fetch_data()
            self.save_data_to_minio(dados) 
        except Exception as e:
            print(f"Erro: {e}")

