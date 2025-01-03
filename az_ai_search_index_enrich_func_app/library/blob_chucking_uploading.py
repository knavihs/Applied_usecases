import json
 
 
class ChuckingUploadingIndex(object):
    def _init_(self,  aiSearch) -> None:
        # self.blob = blob
        self.aiSearch = aiSearch
 
    def upload_document(self, documents: str, index_name: str) -> bool:
        try:
            search_index_client = self.aiSearch.search_client(index_name)
            result = search_index_client.upload_documents(documents=documents)
            print("Upload of new document succeeded: {}".format(result[0].succeeded))
            return result
        except Exception as ex:
            print(ex.message)
    # If the file to be loaded is stored locally   
    def chunckingBlob(self):
        json_data = []
        with open(self.blob) as json_file:
            json_data = json.load(json_file)
        return json_data
   
    def ingestChunk(self, index_name):
        documents = self.chunckingBlob()
        results = self.upload_document(documents, index_name)
        print(f"successfully ingesting to index : {index_name}")
    
    # If we have a dataframe ,then convert it into json object and pass it as json content
    def ingestChunk_pddf(self, index_name,json_content):
        documents = json_content
        results = self.upload_document(documents, index_name)
        print(f"successfully ingesting to index : {index_name} \n and \n results as: {results}")