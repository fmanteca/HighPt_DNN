import pandas as pd
import glob
import os


files = glob.glob("MERGER/*csv")

mylist = []

for filename in files:
    df = pd.read_csv(filename)
    mylist.append(df)
    del df

frame = pd.concat(mylist, ignore_index=True)
frame.to_csv('data_hit_muon_prop.csv', index=False)

