from os import dup
import pandas as pd
import psycopg
import time
def remove_dupilcates(df, dbengine):
    start_date = df.index.min()
    end_date = df.index.max()
    print("------------start and end dates--------------")
    print(start_date, end_date)
    dataFrame       = pd.read_sql("select * from \"generation\"", dbengine)
    print("------------before--------------")
    print(dataFrame)
    with psycopg.connect("dbname=hubertstinia user=hubertstinia") as conn:
        with conn.cursor() as cur:
                cur.execute(f"DELETE FROM generation WHERE index>=\'{start_date}\' and index <=\'{end_date}\'  ;") 
                conn.commit()    
                conn.close()
    dataFrame       = pd.read_sql("select * from \"generation\"", dbengine)
    print("------------ after--------------")
    print(dataFrame)
    time.sleep(1)
    df.to_sql('generation', dbengine,if_exists='append')