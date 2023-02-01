import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3

#connected to database
connector = sqlite3.connect('base.db') 

#convert sqlite database to pandas dataframe
people = pd.read_sql_query("SELECT * FROM people", connector)
card = pd.read_sql_query("SELECT * FROM card", connector)

inner_join = pd.merge(left=people, right=card, left_on=['id'], 
        right_on=['owner'], suffixes=('_l', '_r'))

table = pa.Table.from_pandas(inner_join)

pq.write_to_dataset(
    table,
    root_path='join.parquet',
    partition_cols=['creation_date'],
)