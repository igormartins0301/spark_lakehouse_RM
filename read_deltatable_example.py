# %%
# READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(
    schema='rickmorty',
    tablename_load='count_residents_by_local',
    tablename_save='location',
)
df = ing.load('delta', catalog='gold')

df.show(truncate=False)

# %%
df.select('idEpisodio', 'urlPersonagem').show(n=100, truncate=False)
