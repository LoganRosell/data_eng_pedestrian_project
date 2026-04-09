import numpy as np
import pandas as pd
import os

# courtesy of https://www.geeksforgeeks.org/python/how-to-merge-multiple-csv-files-into-a-single-pandas-dataframe/

path = './pedestrian_project/FARS_csvs'

# files to read:
print(f"Files in the directory: {path}\n")
files = os.listdir(path)
files = [path + '/' + f for f in files if os.path.isfile(path+'/'+f)]
print(*files, sep="\n")

print("Reading and combining...")
df = pd.concat((pd.read_csv(filename, low_memory=False) for filename in files))

print("df shape:", df.shape)

# drop every column we don't need
# columns_keep = [
#     'caseyear','state','st_case','county','cityname',
#     'day','month','year','hour','minute','latitude',
#     'longitud','lgt_condname','weather1name','weather2name',
#     'fatals','drunk_dr'
# ]

print("writing to new csv...")
df.to_csv('FARS_import_all.csv', 
          index=False,
            # columns=columns_keep
          )

print("done.")
