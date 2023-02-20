import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
from configparser import ConfigParser
import logging

logging.basicConfig(
    level=logging.INFO,
    filename='join.log',
    format="{asctime} : {levelname} : {message}",
    style='{'
)

config = ConfigParser()
config.read("config.ini")
config_name = input("Enter config: ")

try:
    config_info = config[config_name]
except KeyError:
    logging.error("No such config exists.")
    config_info = config["DEFAULT"]

def convert ():
    #connected to database
    connector = sqlite3.connect('base.db') 

    try:
        people_str = "SELECT * FROM people"
        card_str = "SELECT * FROM card"

        #convert sqlite database to pandas dataframe
        people = pd.read_sql_query(people_str, connector)
        card = pd.read_sql_query(card_str, connector)
        logging.info("Data from the database was read successfully!")

        inner_join = pd.merge(left=people, right=card, left_on=['id'], 
                right_on=['owner'], suffixes=('_l', '_r'))
        logging.info("Merge was successful!")
     
    except Exception as error:
        logging.warning(error)
    else:
        table = pa.Table.from_pandas(inner_join)

        pq.write_to_dataset(
            table,
            root_path=config_info["path"],
            partition_cols=['creation_date'],
        )
        logging.info("The data is recorded in .parquet format.")
    finally:
        logging.info("The work is finished.")

if __name__ == "__main__":
    convert()