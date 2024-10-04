# Projeto de Ingestão de Dados com Delta Lake
### Objetivo
Este projeto tem como objetivo implementar um pipeline de ingestão de dados utilizando Apache Spark e Delta Lake, com foco na transformação e armazenamento de dados em um ambiente de lakehouse. O pipeline é projetado para processar dados em diferentes camadas (Bronze, Silver e Gold), permitindo operações de ingestão incremental e gerenciamento eficiente de dados.

## Estrutura do Projeto
### Classes Principais
1. Ingestor: Classe base responsável por carregar e salvar dados em formatos Delta ou outros formatos especificados. Ela configura a conexão com o MinIO, um serviço de armazenamento compatível com S3.

2. IngestorCDC: Extensão da classe Ingestor que implementa a lógica de Change Data Capture (CDC). Esta classe é responsável por realizar operações de upsert em uma tabela Delta, permitindo a atualização de registros existentes e a inserção de novos registros com base em um campo de identificação e um timestamp.

3. SilverIngestor: Esta classe realiza a transformação de dados da camada Bronze para a camada Silver, utilizando consultas SQL definidas em arquivos externos. Ela também gerencia a lógica de merge para ingesta incremental, caso necessário.

4. GoldIngestor: Classe que processa dados da camada Silver para a Gold. Ela implementa a lógica de filtrar registros novos com base em uma coluna de data, permitindo a ingesta incremental e o gerenciamento eficiente dos dados armazenados.

## Funcionamento
- Carregamento de Dados: Os dados são carregados da camada Bronze para a Silver utilizando consultas SQL, e posteriormente da camada Silver para a Gold. As classes de ingestão gerenciam a leitura e o salvamento de dados no formato Delta, permitindo versões e controle de dados.

- Transformação de Dados: O pipeline permite a transformação dos dados em cada camada, garantindo que as informações estejam prontas para análise e consumo.

- Ingestão Incremental: A classe GoldIngestor suporta a ingestão incremental, filtrando os dados com base em uma coluna de data para garantir que apenas novos registros sejam adicionados à camada Gold.

## Requisitos
Apache Spark: O projeto requer a instalação do Apache Spark.
Delta Lake: Para suporte ao gerenciamento de dados em formato Delta.
MinIO: Utilizado como um serviço de armazenamento compatível com S3 para armazenamento de dados.
Como Usar
Configure as variáveis de ambiente no arquivo .env com as credenciais do MinIO:

```bash
MINIO_ENDPOINT=<endpoint>
MINIO_ACCESS_KEY=<access_key>
MINIO_SECRET_KEY=<secret_key>
```
Importe as classes no seu código e inicialize o Ingestor ou as classes derivadas (IngestorCDC, SilverIngestor, GoldIngestor) conforme necessário.

Utilize os métodos load, save, e execute_query para interagir com os dados.

Para operações de ingestão, utilize os métodos apropriados nas classes específicas.

### Exemplo de Uso
```bash
ingestor = Ingestor(schema='your_schema', tablename_load='your_table', tablename_save='your_table_save')
df = ingestor.load(data_format='delta', catalog='bronze')
ingestor.save(df, data_format='delta', catalog='silver')
```
### Conclusão
Este projeto proporciona uma base sólida para a construção de pipelines de dados eficientes e escaláveis, permitindo a manipulação de grandes volumes de dados de forma eficaz. A arquitetura em camadas facilita a transformação e o gerenciamento de dados, oferecendo suporte para práticas de governança e qualidade de dados.