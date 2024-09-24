#READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(schema='RickMorty', tablename='episodes')
df = ing.load('delta', catalog='bronze')
df.show(truncate=False)