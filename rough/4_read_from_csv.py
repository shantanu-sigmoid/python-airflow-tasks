from email import header
import pandas as pd

def read_from_csv(path):
    df = pd.read_csv(path,header = 0)
    return df

df = read_from_csv("countries.csv")
print(len(df))
for i in range(len(df)):
    print(df.iloc[i]["state"])
