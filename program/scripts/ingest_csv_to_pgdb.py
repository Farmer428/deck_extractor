import pandas as pd
import numpy as np
from pathlib import Path
from pg_connect import PostgresConnect


pg_db = PostgresConnect()
pg_conn = pg_db.connect("MTG")
engine = pg_db.get_engine()

decklists_directory = Path('D:/decklist csvs/archidekt/')
decklist_file_paths = decklists_directory.iterdir()

for decklist_path in decklist_file_paths:

    decklist_name = Path(decklist_path).stem
    decklist_df = pd.read_csv(decklist_path)

    # Normalize column names
    columns = decklist_df.columns.str.lower()
    decklist_df.columns = columns.str.replace(' ', '_')

    # --- FIX: rename second "collector_number" to "collection_status" ---
    cols = pd.Series(decklist_df.columns)

    for dup in cols[cols.duplicated()].index:
        if cols[dup] == "collector_number":
            cols[dup] = "collection_status"

    decklist_df.columns = cols

    decklist_df = decklist_df.replace('----', np.nan)
    decklist_df.insert(loc=1, column='deck_name', value=decklist_name)

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