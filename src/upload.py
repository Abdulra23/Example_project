import requests, os, time
from decouple import config
from shared import (logger, MIGRATION_PATH,NAME, get_newid)
import pandas as pd

# =================================
# Setup Migration Tool environment:
JSON_DATA_FOLDER_LOCATION = config('JSON_FOLDER')
# =================================
# - Define Uploader constants
UPLOAD_PATH = MIGRATION_PATH + "upload" + os.sep + NAME + os.sep
UPLOAD_REPORT_FILENAME = UPLOAD_PATH + NAME + "_report.xlsx"
# Create folder paths if they do not already exist - makedirs succeeds even if directory exists.
os.makedirs(UPLOAD_PATH, exist_ok=True)


# CONSTANTS
instanaEventAPI = "/api/events/settings/event-specifications/custom"
instanaVersionAPI = "/api/instana/version"
instanaPUTAlertAPI = "//api/events/settings/alerts/"

instanaHeader = {
    "authorization": "apiToken " + config("API_TOKEN"),
    "Content-Type": "application/json",
    "Accept": "application/json",
}


# load JSON files and attempt import to Instana (PUT Event specification with ITM id)
def JSON_import(inputDirectoryPath=JSON_DATA_FOLDER_LOCATION) -> int:
    iterator = 0
    if os.path.isdir(inputDirectoryPath) and os.path.exists(inputDirectoryPath):
        for folder, subs, files in os.walk(inputDirectoryPath):
            for file in files:
                if file.endswith(".json"):
                    upload_single_file(folder, file)
                    iterator+=1
    else:
        logger.error("Please provide a valid directory path.")
        return None
    return iterator

def upload_single_file(folder, file) -> str:
    ''' Upload a singe file from the given folder.
        Expects a folder path and a file name.
        Returns newly created event object.
    '''
    logger.info("Folder: "+str(folder))
    logger.info("File: "+str(file))

    with open(os.path.join(folder, file), 'r') as jsonFile:
        # get ID to use as eventSpecificationId on PUT request
        jsonToUpload = None
        jsonToUpload = pd.read_json(jsonFile)
        if jsonToUpload.empty:
            logger.error("System cannot read data in file: "+ str(jsonFile.name))
            return

        id = jsonToUpload["id"].values[0]
        logger.info("Uploading " + str(file) + " ID: " + str(id))
        if id:
            # reload the JSON from file again 
            # as above id logic seems to corrupt the json data sent with request
            with open(os.path.join(folder, file), 'r') as jsonFile:                       
                res = requests.put(
                    config('BASE_URL') + instanaEventAPI + "/" + id,
                    headers=instanaHeader,
                    verify=False,
                    data=jsonFile,
                )  
                if res.status_code == 200:
                    logger.info("Upload successful: " + str(res.status_code))
                    return(res.content)
                else:
                    logger.error("Upload failed: " + str(res.status_code))                                           
                    for key, value in res.json().items():                
                        if key == "errors" and len(value) > 0:
                            logger.error(key + ": " + str(value))
                    logger.info("Trying to re-create the event for "+str(jsonFile))
                    with open(os.path.join(folder, file), 'r') as jsonFile: 
                        res = requests.post(
                            config('BASE_URL') + instanaEventAPI,
                            headers=instanaHeader,
                            verify=False,
                            data=jsonFile,
                        )                                    
                        if res.status_code == 200:
                            logger.info("Upload successful: " + str(res.status_code))
                            return(res.content)
                        else:
                            logger.error("Re-Uploading failed: " + str(res.status_code))                                           
                            for key, value in res.json().items():                
                                if key == "errors" and len(value) > 0:
                                    logger.error(key + ": " + str(value))
        else:
            logger.error("Error processing the file. Upload terminated. No event id found")
    return None

#TODO: test this functionallity and refactor
def upload_alert(folder, file):
    #get a 16 digit unique id 
    id = get_newid(key_char_length=16)
    logger.info("Uploading " + str(file) + " ID: " + str(id))
    if id:
        # reload the JSON from file again 
        # as above id logic seems to corrupt the json data sent with request
        logger.info("Trying to update/create an alert:")
        with open(os.path.join(folder, file), 'r') as jsonFile:
            res = requests.put(
                                    config('BASE_URL') + instanaPUTAlertAPI + id,
                                    headers=instanaHeader,
                                    verify=False,
                                    data=jsonFile,
                                )
            logger.info(res.content)

# Test connection to Instana & upload events
def testConnection():
    try:
        testConnection = requests.get(
            config('BASE_URL') + instanaVersionAPI, headers=instanaHeader, verify=False
        )
        logger.info("Testing connection to: " + config('BASE_URL'))
        if testConnection.ok:
            logger.info("Instana Version: " + testConnection.text)
        return testConnection
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def importData():
    configType = "events"
    
    logger.info("Importing Event Specifications from: " + JSON_DATA_FOLDER_LOCATION)    
    JSON_import(JSON_DATA_FOLDER_LOCATION)


# Testing
# start_time = time.time()
# test = testConnection()
# if test.ok:
#     upload_alert(JSON_DATA_FOLDER_LOCATION,'MQ_ASD_DP_Q1BUL_InputOpen_Warn_alert.json')
# else: 
#     logger.error("ERROR! failed to import data.")
# logger.info("Total execution time: " + str(round(time.time() - start_time, 3)) + " seconds")