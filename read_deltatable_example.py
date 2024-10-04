# %%
# READ DELTA TABLE EXAMPLE
from libs.ingestor import Ingestor

ing = Ingestor(
    schema='rickmorty',
    tablename_load='count_episodes_by_year',
    tablename_save='episode',
)
df = ing.load('delta', catalog='gold')

df.show(n=1000)
