
import os
import pandas as pd
# import geopandas as gpd
import numpy as np
import time
import meilisearch
from datetime import datetime

from typing import Dict


def ingest_data(index, df: pd.DataFrame, index_uid: str):
    documents = df.to_dict(orient="records")
    task = index.add_documents(documents, index_uid)
    
    task_id = task.task_uid

    task_status = index.get_task(task_id)
    while task_status.status not in ["succeeded", "failed"]:
        time.sleep(1)
        task_status = index.get_task(task_id)

    print(f"Task Status: {task_status.status} -- {datetime.utcnow()}")
    if task_status.status == "failed":
        print(f"Error message: {task_status.error['message']}")



def ingest_index(index_name: str, df: pd.DataFrame, index_uid: str, client: meilisearch.Client):
    '''
    
    '''
    df = df.replace({np.nan: ""})
    
    index = client.index(index_name)
    client.create_index(index_name, {'primaryKey': index_uid})

    rows = df.shape[0]
    
    if rows > 10000:
        step = 10000
        for e in range(0, rows, step):
            ingest_data(index=index, df=df[e: e+step], index_uid=index_uid)
    
    ingest_data(index=index, df=df, index_uid=index_uid)


def load_to_meili(df: pd.DataFrame, index_name: str, client: meilisearch.Client) -> Dict:
    #from sqlalchemy import create_engine
    
    #_engine = create_engine(engine) 
    #_gdf = gdf.copy()
    #_gdf.to_postgis(index_name, _engine)
    
    df = df.copy()
    df = df.replace({np.nan: ''})

    #client = meilisearch.Client("http://185.252.235.89:7700", 'aSampleMasterKey')
    # client = meilisearch.Client("http://localhost:7700", '1234567890')
    
    ingest_index(index_name=index_name, df=df, index_uid='uid', client=client)
    
    return {'status': "Done", 'with_success': True, "df": df}



if __name__ == "__main__":
    
    BASE_PATH = "/mapslabio/aquagis/meilisearch-service/source_data/"

    # Load data
    ll = [
        {
            "search_spine": "parcels_pvn_2009",
            "data_source": 'parcels_pvn_2009',
            "data_lineage": []
        },
        {
            "search_spine": "water_meters_pleven",
            "data_source": 'water_meters_20241007_uid',
            "data_lineage": []
        }
    ]

    dfs = []
    for e in ll:
        obj = {
            'df': pd.read_parquet(os.path.join(BASE_PATH, f"{e.get('data_source')}.parquet"))
        }
        o = {**e, **obj}
        dfs.append(o)

    # Create index
    client = meilisearch.Client("http://localhost:7700", '1234567890')

    for d in dfs:
        load_to_meili(d.get('df'), d.get('search_spine'), client=client)

