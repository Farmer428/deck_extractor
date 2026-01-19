import os
import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine
import time

class PostgresConnect:
    def __init__(
            self,
            user=None,
            password=None,
            host=None, 
            port=None,
            dbname=None,
            max_retries=3, 
            retry_delay=3
    ):
        self.user = user or os.getenv("PG_USER")
        self.password = password or os.getenv("PG_PW")
        self.host = host or os.getenv("PG_HOST")
        self.port = port or os.getenv("PG_PORT")
        self.dbname = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None

    def connect(self, db):
        counter = 1
        while counter <= self.max_retries:
            try:
                self.connection = pg.connect(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    dbname=db
                )
                conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{db}"
                self.engine = create_engine(conn_str)
                return self.connection
            except Exception as e:
                if counter < self.max_retries:
                    counter +=1
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise e
    def get_connection(self):
        if self.connection is None:
            self.connect()
        return self.connection
    
    def get_engine(self):
        if self.engine is None:
            self.get_connection()
        return self.engine
    
    def query(self, query, connection=None):
        engine = self.get_engine()
        try:
            return pd.read_sql(query, engine)
        except Exception as e:
            raise e
    
    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection=None
        if self.engine:
            try:
                self.engine.dispose()
            except Exception:
                pass
            self.engine=None
    