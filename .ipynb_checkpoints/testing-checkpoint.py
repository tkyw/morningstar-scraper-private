import pandas as pd
from IPython.display import display

df = pd.read_csv("Malaysia Fund Universe.csv", index_col=0)
display(df.sort_index())
