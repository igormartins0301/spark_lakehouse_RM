#%%
#READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(schema='rickmorty', tablename='episode')
df = ing.load('delta', catalog='silver')
#df.show(truncate=False)

#%%
df.show(n=1000)