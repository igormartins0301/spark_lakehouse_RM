#%%
#READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(schema='rickmorty', tablename_load='character', tablename_save='episode_character')
df = ing.load('delta', catalog='silver')

df.show(truncate=False)

#%%
df.select('idEpisodio', 'urlPersonagem').show(n=100, truncate=False)