import argparse
parser = argparse.ArgumentParser()

#-db DATABSE -u USERNAME -p PASSWORD -size 20
parser.add_argument("-csv", "--csv", help="CSV File")
parser.add_argument("-json", "--json", help="Json File")
args = parser.parse_args()

import pyarrow.csv as pv
import pyarrow.json as pj
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import pathlib
filename = args.csv
jfilename = args.json
if filename is not None:
    file_extension = pathlib.Path(filename).suffix
    table = pv.read_csv(filename,parse_options=pv.ParseOptions(newlines_in_values=True))
    pq.write_table(table, filename.replace(file_extension, '.parquet'),compression='NONE')
if jfilename is not None:
    file_extension = pathlib.Path(jfilename).suffix
    table = pj.read_json(jfilename,parse_options=pj.ParseOptions(newlines_in_values=True))
    pq.write_table(table, jfilename.replace(file_extension, '.parquet'),compression='NONE')