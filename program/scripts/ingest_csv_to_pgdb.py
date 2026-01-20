import pandas as pd
from pg_connect import PostgresConnect


pg_db = PostgresConnect()
pg_conn = pg_db.connect("MTG")
engine = pg_db.get_engine()

decklist_file_path = 'D:/decklist csvs/archidekt/aloy_(i got nothin).csv'
decklist_df = pd.read_csv(decklist_file_path)

# print(decklist_df)
try:
    decklist_df.to_sql(
        name="archidekt",
        con=engine,
        schema="raw_decklist",
        if_exists="replace",
        index=False
    )
    print("successfully uploaded csv")
except:
    print("could not load csv")

pg_conn.close()