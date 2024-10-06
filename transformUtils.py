
# PDTs with MISSING or MS_OFFLINE == System Event, otherwise it should be a Built-In Event
def getEventType(pdtString):
    if pdtString == "MISSING" or pdtString == "MS_OFFLINE":
        return "SYSTEM"
    else:
        return "BUILTIN"

def getName(xmlFile):
    return "Test_Situation"

def getSeverity(pdtString):
    return 10

# only migrating if AUTOSTART=*YES
def getEnabled(xmlFile):
    return True

def getDescription(text):
    return "Test Situation description"

def getEntityType(sitname):
    return "db2Database"

def getMetric(pdtString):
    return "databases.connectionsCount"


