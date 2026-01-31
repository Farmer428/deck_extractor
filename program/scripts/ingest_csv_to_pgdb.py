import pandas as pd
import numpy as np
from pg_connect import PostgresConnect


pg_db = PostgresConnect()
pg_conn = pg_db.connect("MTG")
engine = pg_db.get_engine()

decklist_file_path = 'D:/decklist csvs/archidekt/aloy_(i got nothin).csv'
decklist_df = pd.read_csv(decklist_file_path)
columns = decklist_df.columns.str.lower()
decklist_df.columns = columns.str.replace(' ', '_')
decklist_df = decklist_df.replace('----', np.nan)

# print(decklist_df['price'])

try:
    decklist_df.to_sql(
        name="archidekt",
        con=engine,
        schema="raw_decklist",
        if_exists="append",
        index=False
    )
    print("successfully uploaded csv")
except Exception as e:
    print("could not load csv")
    print(e)

pg_conn.close()