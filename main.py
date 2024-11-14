import fastapi as fa
import pydantic
from silobuster_test.libs.connector.postgres_connector import PostgresConnector
from database import Database
from hardcoded import HardCoded
db = Database('wa211whatcomGroup',['whatcomSep2024','wa211Sep2024'])
app = fa.FastAPI()
import pandas as pd

#------------------------------------------------------------------------
#Data structures
class Valadate_Ids(pydantic.BaseModel):
    type: str
    ids: list
    cluster_id:str
class Data_Sources(pydantic.BaseModel):
    data_sources: list
    type: str

#------------------------------------------------------------------------
#Root
@app.get("/")
async def root():
    return {"message": "Hello World"}


#------------------------------------------------------------------------
#Models
@app.get('/models')
async def getModels():
    return HardCoded.models
async def cluster(model_name,job_id,count:int,pagination:int):
    df = db.get_by_model(model_name,job_id)
    dic = {}
    for num in range(0,len(df.keys())):
        if num >= (count)*(pagination+1):
            continue
        if num <= (count*(pagination))-1:
            continue
        lis =[]
        
        for key in df.keys():
            
            lis.append({key : df[key][num]})
        dic[df["id"][num]] = lis
    return dic
#------------------------------------------------------------------------
#Clusters
@app.post('/clusters')
async def valadate(res: Valadate_Ids):
    if res.type == 'grouped':
        db.create_aproved_events(res.ids,res.cluster_id)
        for id in res.ids:
            db.save_verified_group_member(id,res.cluster_id)
    elif res.type =='ungrouped':
        db.create_rejected_events(res.ids,res.cluster_id)
    elif res.type =='unknown':
        db.create_viewed_events(res.ids,res.cluster_id)
    else:
        return fa.HTTPException(404,'unknown type')
    return "succesfully valadated"
@app.get('/clusters')
async def all_clusters(job_id,count:int,pagination:int):
    df = db.get_all_clusters(job_id)
    dic = {}
    for num in range(0,len(df.keys())):
        if num >= (count)*(pagination+1):
            continue
        if num <= (count*(pagination))-1:
            continue
        lis =[]
        
        for key in df.keys():
            
            lis.append({key : df[key][num]})
        dic[df["id"][num]] = lis
    return dic
#------------------------------------------------------------------------
#Data Sources
@app.get('/data-sources')
async def get_data_sources():
    return HardCoded.databases

@app.post('/data-sources')
async def create_job(data_sources: Data_Sources):
        if data_sources.type == "organization":
            pass
        elif data_sources.type == "service":
            pass
        else:
            return fa.HTTPException(404,'invalid type')


#------------------------------------------------------------------------
#Reports
@app.get('/reports')
async def get_report_types():
    return HardCoded.reportTypes