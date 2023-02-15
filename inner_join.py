import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3

if __name__ == "__main__":
    #connected to database
    connector = sqlite3.connect('base.db') 

    people_str = "SELECT * FROM people"
    card_str = "SELECT * FROM card"

    #convert sqlite database to pandas dataframe
    people = pd.read_sql_query(people_str, connector)
    card = pd.read_sql_query(card_str, connector)

    inner_join = pd.merge(left=people, right=card, left_on=['id'], 
            right_on=['owner'], suffixes=('_l', '_r'))

    table = pa.Table.from_pandas(inner_join)

    pq.write_to_dataset(
        table,
        root_path='join.parquet',
        partition_cols=['creation_date'],
    )