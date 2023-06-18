import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
import os
from bson import ObjectId
import time

load_dotenv()
def get_db_client() -> MongoClient:
    db_client: MongoClient = MongoClient(os.environ["CONNECTION_STRING"])
    return db_client

def load_to_db(df, country):
    start = time.time()
    meta_acc = 'Metadata_Acceptance'
    dp_acc = 'Datapoint_Acceptance'
    client: MongoClient = get_db_client()
    db = client.Stromzeiten_dev

    dp_collection = db[dp_acc]
    meta_collection = db[meta_acc]
    tags = df.columns
    if not df.empty:
        for tag in tags:
            column = df[tag]
            x = meta_collection.find_one({"type": tag})
            for indx, value in column.items():
                data: dict[str, str] = {
                    "metadataid": ObjectId(x.get('_id')),
                    "postedById": ObjectId('637912d934603726adcbc31c'),
                    "value": value,
                    "timestamp": indx,
                    "country": country
                }
                timestamp=str(indx)
                timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S%z')
                query = { "timestamp": timestamp, 
                "metadataid": ObjectId(x.get('_id')), "country": country }
                find_duplicate= db["Datapoint_Acceptance"].find_one(query)
                
                if find_duplicate is None:
                    result = dp_collection.insert_one(data)
                    
                else:
                    result = dp_collection.replace_one(find_duplicate ,data)
                    
    else:
        print('Nothing to add!')
    end = time.time()
    elapsed_time = end-start
    return elapsed_time

def load_forecast_to_db(df, country):
    start = time.time()
    dp_acc = 'Datapoint_Forecast'
    client: MongoClient = get_db_client()
    db = client.Stromzeiten_dev

    dp_collection = db[dp_acc]
    tags = df.columns
    if not df.empty:
        for date, val in df['Cei_prediction'].items():
            data: dict[str, str] = {
                "value": val,
                "timestamp": date,
                "country": country
            }
            timestamp=str(date)
            timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S%z')
            query = { "timestamp": timestamp, "country": country }
            find_duplicate= db["Datapoint_Forecast"].find_one(query)
            if find_duplicate is None:
                result = dp_collection.insert_one(data)   
            else:
                result = dp_collection.replace_one(find_duplicate,data)             
    else:
        print('Nothing to add!')
    end = time.time()
    elapsed_time = end-start
    return elapsed_time