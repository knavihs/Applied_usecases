import library.common as const
from azure.identity import  ClientSecretCredential, DefaultAzureCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
 
## AiSearch Class to use Aisearch clients as objects
 
class AiSearch(object):
 
    def _init_(self, spncredential):
        self.spncredential = spncredential
 
    def search_client(self, index_name) -> SearchClient:
        search_client = SearchClient(
            endpoint=const.AZURE_SEARCH_SERVICE_ENDPOINT,
            index_name=index_name,
            credential=self.spncredential
        )
        return search_client
 
    def search_index_client(self) -> SearchIndexClient:
        search_index_client = SearchIndexClient(
            endpoint=const.AZURE_SEARCH_SERVICE_ENDPOINT,
            credential=self.spncredential
        )
        return search_index_client
 
    def search_indexer_client(self) -> SearchIndexerClient:
        search_indexer_client = SearchIndexerClient(
            endpoint=const.AZURE_SEARCH_SERVICE_ENDPOINT,
            credential=self.spncredential
        )
        return search_indexer_client