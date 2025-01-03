from azure.identity import ManagedIdentityCredential,DefaultAzureCredential,ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import json
from io import StringIO
 
 
class keyvault():
    def _init_(self,kv_uri,env) -> None:
        self.kv_uri = kv_uri
        self.env = env
 
    def get_cred(self):
        # credential = ManagedIdentityCredential(
        #                 client_id=self.managed_client_id,
        #                 )
        if self.env == 'Azure':
            credential = DefaultAzureCredential()
        elif self.env == 'Local':
            import common as  co
            credential = ClientSecretCredential(client_id=co.client_id,client_secret=co.client_secret,tenant_id=co.tenant_id)
 
        return credential
   
    def kvclient(self):
        cred = self.get_cred()
        client = SecretClient(vault_url=self.kv_uri, credential=cred)
        return client
 
    def get_kv_secret(self,secretName:str):
        client = self.kvclient()
        retrieved_secret =  client.get_secret(secretName)
        return retrieved_secret
    
class blobcall():
    def _init_(self,acc_url,kv_client_id,kv_client_secret,kv_tenant_id) -> None:
        self.acc_url = acc_url
        self.kv_client_id = kv_client_id
        self.kv_client_secret = kv_client_secret
        self.kv_tenant_id = kv_tenant_id
 
    def cred_gen(self):
        credential = ClientSecretCredential(client_id=self.kv_client_id,client_secret=self.kv_client_secret,tenant_id=self.kv_tenant_id)
        return credential
 
    def blob_service_client(self):
        credential = self.cred_gen()
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(self.acc_url, credential=credential)
        return blob_service_client
   
    def read_blob_pd_df(self,container_name,blob_name):
        blob_service_client = self.blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # Download the blob content as a string
        blob_data = blob_client.download_blob().content_as_text()
        # Load the content into a Pandas DataFrame
        df = pd.read_csv(StringIO(blob_data))
        return df
   
    def read_blob_to_json(self,container_name,blob_name):
        blob_service_client = self.blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # Download the blob content as a string
        blob_data = blob_client.download_blob().content_as_text()
        # Load the content into a Pandas DataFrame
        df = pd.read_csv(StringIO(blob_data))
        json_content = df.to_dict(orient='records')
        return json_content