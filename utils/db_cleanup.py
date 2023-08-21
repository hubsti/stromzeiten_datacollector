from os import dup
import pandas as pd
import psycopg
import time
def remove_dupilcates(df, dbengine, country_code):
    start_date = df.index.min()
    end_date = df.index.max()
    print("------------start and end dates--------------")
    print(start_date, end_date)
    dataFrame       = pd.read_sql("select * from \"generation_acc\"", dbengine)
    print("------------before--------------")
    print(dataFrame)
    with psycopg.connect("dbname=stromzeiten user=admin host=localhost password=admin") as conn:
        with conn.cursor() as cur:
                cur.execute(f"DELETE FROM generation_acc WHERE index>=\'{start_date}\' and index <=\'{end_date}\' and generation_acc.country_code = \'{country_code}\' ;") 
                conn.commit()    
                conn.close()
    dataFrame       = pd.read_sql("select * from \"generation_acc\"", dbengine)
    print("------------ after--------------")
    print(dataFrame)
    time.sleep(1)
    df.to_sql('generation_acc', dbengine,if_exists='append')
