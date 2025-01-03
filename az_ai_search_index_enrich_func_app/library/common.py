## AI - Search
AZURE_SEARCH_SERVICE_ENDPOINT : str = ""
AZURE_SEARCH_INDEX_NAME : str = ""
 
## TESTING
INDEX_NAME : str = ""
 
## OPEN-AI / KONG
## LLM Model access through Kong API Gateway
kong_project : str = ""
kong_client_id : str = "" #LLM Client Id
kong_client_secret : str = "" # LLM Client Secret
oauth_endpoint : str = "" #Oauth Endpoint
grant_type : str = "client_credentials"
scope : str = ""
kong_base_url : str = "" #LLM Endpoint
llm_model : str = "gpt-35-turbo-16k" #LLL deployment model
embedding_model : str = "text-embedding-ada-002" #LLM embedding model
 
## Azure service pincipal 
tenant_id : str = ""
client_id : str = ""
client_secret : str = ""