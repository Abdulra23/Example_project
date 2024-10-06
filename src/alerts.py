import requests, os, json, secrets, string
from shared import logger, get_newid
from decouple import config

# read necessary env variables
ALERT_JSON_DESTINATION=config('ALERT_JSON_DESTINATION')

# CONSTANTS
instanaEventAPI = "/api/events/settings/event-specifications/custom"
instanaPUTAlertChannelAPI = "//api/events/settings/alertingChannels/" 
instanaGETAllAlertChannelsAPI = "//api/events/settings/alertingChannels"
instanaGETAllAlerts = "//api/events/settings/alerts"

instanaVersionAPI = "/api/instana/version"
instanaHeader = {
    "authorization": "apiToken " + config("API_TOKEN"),
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Test connection to Instana & upload configurations
def testConnection():
    try:
        testConnection = requests.get(
            config('BASE_URL') + instanaVersionAPI, headers=instanaHeader, verify=False
        )
        logger.info("Testing connection to: " + config('BASE_URL'))
        if testConnection.ok:
            logger.info("Instana Version: "+ testConnection.text)
        return testConnection
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def create_alert_channel():
    # TODO: this method is only for email alert channel creation.
    #  The code must support all other available alert channell creation capability too. 

    #get a 16 digit unique id 
    alert_id = get_newid(key_char_length=16)
    jsonAlertConfig={
        "id": alert_id,
        "name":"11-July Test Email",
        "emails":["test@test.org"],
        "kind":"EMAIL"
        }
    jsonString = json.dumps(jsonAlertConfig, indent=4)
    logger.info("Trying to update/create an alert channel:")
    res = requests.put(
                            config('BASE_URL') + instanaPUTAlertChannelAPI + alert_id,
                            headers=instanaHeader,
                            verify=False,
                            data=jsonString,
                        )
    logger.info(res.content)

def get_all_alert_channels():
    res = requests.get(
                            config('BASE_URL') + instanaGETAllAlertChannelsAPI,
                            headers=instanaHeader,
                            verify=False
                        )
    logger.info(res.content)



def create_alert(alert_name=' ', events_list=[], alert_channels_list=[], custom_payload_fields=[]):
    ''' Creates new or updates an existing alert configurations in Instana tenant.
        args: list of events, list of alert channels, and name of the alert.
        returns: newly created alert object.
    '''
    # #get a 16 digit unique id 
    # alert_id = get_newid(key_char_length=16)

    jsonAlertConfig={
        # "id":alert_id,
        "alertName":alert_name,
        "muteUntil":0,
        # integrationIds are the list of alert channel ids (make sure they are created before referenced)
        "integrationIds":alert_channels_list,
        "eventFilteringConfiguration":{
            # eventFilteringConfiguration.query is the DFQ value on the alerts config
            "query":"entity.tag:Ben",
            # eventFilteringConfiguration.ruleIds is object holding id of the events associated
            "ruleIds":events_list,
            "applicationAlertConfigIds":[]
            },
        "customPayloadFields":custom_payload_fields
        }

    # use four indents to make it easier to read the result:
    jsonString = json.dumps(jsonAlertConfig, indent=4)
    with open(ALERT_JSON_DESTINATION+alert_name+".json", "w") as jsonFile:
        jsonFile.write(jsonString)
    

def get_all_alerts():
    res = requests.get(
                            config('BASE_URL') + instanaGETAllAlerts,
                            headers=instanaHeader,
                            verify=False
                        )
    logger.info(res.content)


# Testing
# test = testConnection()
# if test.ok: 
#     #get_all_alert_channels()
#     #create_alert_channel()
#     #get_all_alerts()
#     # alertChannel_1 = "aM5eJezJkemP480C"
#     # alertChannel_2 = "MHCsTlj7nuwhbB1W"
#     # event_1 = "v3dkrP9EVzuCTr-3"
#     # event_2 = "OlLmO-yLQOmEZiOIIjNx4Q"
#     logger.info("creating an alert")
#     # create_alert(alert_name="Muhammad_test_alert", events_list=[event_1,event_2],alert_channels_list=[alertChannel_1, alertChannel_2],custom_payload_fields=[])
# else: 
#     logger.error("ERROR! failed to create/update alerts data.")