import azure.functions as func
import logging
import json
import requests
from library.blob_chucking_uploading import  ChuckingUploadingIndex
from library.aisearch_client import AiSearch
from library.common_clients import blobcall,keyvault
import re
import numpy as np
import pandas as pd
import library.common as const

 
app = func.FunctionApp()
 
@app.function_name('sampleEventgridTrigger')
@app.event_grid_trigger(arg_name="azeventgrid")
def event_grid_trigger(azeventgrid: func.EventGridEvent):    
    logging.info('Python EventGrid trigger processed an event')    
    event_data = azeventgrid.get_json()    # Extracting necessary details from the event    
    event_type = azeventgrid.event_type    
    blob_url = event_data.get('url', '')    
    blob_path = blob_url.split('?')[0]  # Remove SAS token if present  
    logging.info('blob_path is : %s',json.dumps({'blob_url':blob_url,'blob_path':blob_path}))   
    # blob_name = blob_path.split('/')[-1]  # Extract blob name from the path
    pattern = re.compile(r"https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)")
    match = pattern.match(blob_path)
    if match:
        storage_account_name = match.group(1)
        container_name = match.group(2)
        blob_file_path = match.group(3)
 
    result = json.dumps({
             'id': azeventgrid.id,
             'data': event_data,        
             'topic': azeventgrid.topic,        
             'subject': azeventgrid.subject,        
             'event_type': event_type,        
             'blob_url': blob_path,        
             'blob_name': blob_file_path,
             'storage_account_name': storage_account_name,
            'container_name': container_name    }
             )
    logging.info('Python EventGrid trigger processed an event: %s',result)
 
    # Creating Key valut client
    kv_obj = keyvault(kv_uri='https://keyvault_name.vault.azure.net/',env='Azure')
    client_id = kv_obj.get_kv_secret('adls-client-id')
    client_secret = kv_obj.get_kv_secret('adls-client-secret')
    tenant_id = kv_obj.get_kv_secret('tenant-id')
    account_url = f"https://{storage_account_name}.blob.core.windows.net"
 
    logging.info('connected to Key valut for fetching credential for : %s',client_id)
 
    # creating Blob service client
    blob_obj = blobcall(account_url,client_id,client_secret,tenant_id)
    df = blob_obj.read_blob_pd_df(container_name,blob_file_path)
 
    logging.info('blob read into pandas dataframe with dimension as : %s',df.shape)
    # Create the 'id' column with random increasing numbers converted to string 
    df['id'] = (np.arange(len(df)) + 1).astype(str)
    # transformation as per the schema of the AI Search Indexes
    
    new_df = df # Apply transformation Logic on df pandas dataframe and select the column as present in the fields of AI search Indexes
 
    json_list = new_df.to_dict(orient='records')
    logging.info('sample of json that will fed in AISearch : %s',json_list[0])
 
    credential = blob_obj.cred_gen()        
       
    
    aisearch = AiSearch(credential)
    checkuploadindex = ChuckingUploadingIndex(aisearch)
    for document in json_list:
        checkuploadindex.ingestChunk_pddf(index_name=const.AZURE_SEARCH_INDEX_NAME,json_content=document)
 
    
    
    logging.info('New blob data has been uploaded into ai search indexer')