import argparse
parser = argparse.ArgumentParser()

#-db DATABSE -u USERNAME -p PASSWORD -size 20
parser.add_argument("-csv", "--csv", help="CSV File")

args = parser.parse_args()

import pyarrow.csv as pv
import pyarrow.parquet as pq
filename = args.csv
table = pv.read_csv(filename)
pq.write_table(table, filename.replace('csv', 'parquet'),compression='NONE')