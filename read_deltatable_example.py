# %%
# READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(
    schema='rickmorty',
    tablename_load='episode_character',
    tablename_save='episode',
)
df = ing.load('delta', catalog='silver')

df.show(n=1000)
