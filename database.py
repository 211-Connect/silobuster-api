from silobuster_test.libs.connector.postgres_connector import PostgresConnector
import pandas as pd
from queries import Queries
import json
from uuid import uuid4
from dotenv import load_dotenv
load_dotenv()
import os
groupschemaName = 'wa211whatcomGroup'
schemas_for_data:list = ['whatcomSep2024','wa211Sep2024']
class Database():
    """Handles interaction with the Postgres database where everything is stored"""
    def __init__(self,groupschemaName:str,schemas_for_data:list) -> None:
        self.postgres_connecter:PostgresConnector = PostgresConnector(database = os.environ.get("defaultdb"),
            user = os.environ.get('user'),
            password = os.environ.get('password'),
            host = os.environ.get('host'),
            port = os.environ.get('port'),
            )
        self.groupschemaName = groupschemaName
        self.schemas_for_data = schemas_for_data
        self.last_used_query = None
    def get_all_clusters(self,job_id) -> pd.DataFrame:
        """returns a dataframe for verification"""
        dfList:list = []
        for schema in self.schemas_for_data:
            self.postgres_connecter.cursor.execute(Queries.allClusters(schema,groupschemaName,job_id))
            rows = self.postgres_connecter.cursor.fetchall()
            records_column_names = [desc[0] for desc in self.postgres_connecter.cursor.description]
        
        dfList.append(pd.DataFrame(rows, columns=records_column_names))
        df_orgs_in_clusters = pd.concat(dfList)
        df_orgs_in_clusters = df_orgs_in_clusters.reset_index(drop=True)
        self.last_used_query = df_orgs_in_clusters
        return df_orgs_in_clusters
    def get_by_model(self,model_name,job_id) -> pd.DataFrame:
        """returns a dataframe for verification"""
        dfList:list = []
        for schema in self.schemas_for_data:
            self.postgres_connecter.cursor.execute(Queries.byModel(schema,self.groupschemaName,model_name,job_id))
            rows = self.postgres_connecter.cursor.fetchall()
            records_column_names = [desc[0] for desc in self.postgres_connecter.cursor.description]
        
        dfList.append(pd.DataFrame(rows, columns=records_column_names))
        df_orgs_in_clusters = pd.concat(dfList)
        df_orgs_in_clusters = df_orgs_in_clusters.reset_index(drop=True)
        self.last_used_query = df_orgs_in_clusters
        return df_orgs_in_clusters
    def _create_log(self,event_type, message,model_name =None, confidence=None):
        new_dict =  {"type":event_type, "message": message}
        if (model_name is not None):
            new_dict['model_name'] = model_name
        if (confidence is not None):
            new_dict['confidence'] = confidence

        json_str = json.dumps(new_dict)
        return json_str
    def _createEvent(self,location_id,cluster_id,member_id,event_type,message,model_name=None,confidence=None):
        log = self._create_log(event_type,message,model_name,confidence)
        # self.postgres_connecter.cursor.execute(f'''INSERT INTO "{groupschemaName}".event (ref_table,member_entity,member_id,log,job_id,ref_id,location_id) values ('cluster','organization','{member_id}','{log}','{job_id}','{cluster_id}','{location_id}')''')
        # self.postgres_connecter.cursor.execute('COMMIT')
    def create_rejected_events(self,ids:list,cluster_id):
        for id in ids:
            row:pd.DataFrame = self.last_used_query[id]
            loc_id = row.first()['location_id']
            self._createEvent(loc_id,cluster_id,id,'organization','Member rejected.')
    def create_viewed_events(self,ids:list,cluster_id):
        for id in ids:
            row:pd.DataFrame = self.last_used_query[id]
            loc_id = row.first()['location_id']
            self._createEvent(loc_id,cluster_id,id,'organization','Member viewed.')
    def create_aproved_events(self,ids:list,cluster_id):
        for id in ids:
            row:pd.DataFrame = self.last_used_query[id]
            loc_id = row.first()['location_id']
            self._createEvent(loc_id,cluster_id,id,'organization','Member affirmed.')
    def create_organization_job(self):
        dfList:list = []
        for schema in self.schemas_for_data:
            self.postgres_connecter.cursor.execute(Queries.byModel(Queries.organization_cluster(schema)))
            rows = self.postgres_connecter.cursor.fetchall()
            records_column_names = [desc[0] for desc in self.postgres_connecter.cursor.description]
        
        dfList.append(pd.DataFrame(rows, columns=records_column_names))
        df_orgs_in_clusters = pd.concat(dfList)
        df_orgs_in_clusters = df_orgs_in_clusters.reset_index(drop=True)
        self.last_used_query = df_orgs_in_clusters
        return df_orgs_in_clusters    
    def save_verified_group_member(self,link_id,cluster_id):
        query = f"""INSERT INTO "{groupschemaName}"."verified_group_member" (id,link_id,link_entity,verified_group_id)
        values ('{uuid4()}', '{link_id}','organization','{cluster_id}')"""
        self.postgres_connecter.cursor.execute(query)
        self.postgres_connecter.cursor.execute('COMMIT')
        
