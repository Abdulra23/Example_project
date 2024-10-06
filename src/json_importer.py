import json,requests,os

testData = {
  "sitname": "LOUISE_Test_from_Python2withID",
  "entityType": "db2Database",
  "metricName": "databases.connectionsCount"
}

BaseURL="https://jflegr1-instana.fyre.ibm.com"
instanaAPI="/api/events/settings/event-specifications/custom"
instanaVersionAPI="/api/instana/version"

instanaHeader = {
    "authorization": "apiToken q2GpJn9JRQaRZQAgF9u4zQ",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

rootFolder="data/json/"


# Get Custom events
# customEvents=requests.get(BaseURL+instanaAPI, headers=instanaHeader,verify=False)

# Post Event configuration - TESTING
# jsonEventFile = open("data/json/events/data.json", "r")
# res=requests.post(BaseURL+instanaAPI, headers=instanaHeader,verify=False,data=jsonEventFile)
# print(res)

# load JSON files and attempt import to Instana (POST Event specification )
def JSON_import(inputDirectoryPath)-> int:
    iterator = 0
    if os.path.isdir(inputDirectoryPath) and os.path.exists(inputDirectoryPath):
        for folder, subs, files in os.walk(inputDirectoryPath):
                for file in files:
                    if file.endswith('.json'):
                        print(file)
                        with open(os.path.join(folder, file), 'r') as jsonFile:
                            res=requests.post(BaseURL+instanaAPI, headers=instanaHeader,verify=False,data=jsonFile)
                            print(res)
    else:
        print("Please provide a valid directory path.")
        return None    
    print("Execution Finished !!")
    return iterator

# Test connection to Instana
testConnection=requests.get(BaseURL+instanaVersionAPI, headers=instanaHeader,verify=False)
print("Testing connection to ..." + BaseURL)
if testConnection.ok:
    print("Instana Version: ", testConnection.text)
    print("Importing Event Specifications...")
    configType="events"
    JSON_import(rootFolder+"/"+configType)