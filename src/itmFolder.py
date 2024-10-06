import json

class ITMFolder:
    system_count = 0
    
    def __init__(
        self,
        # fname,
        folderId=None,
        supported=None,
        description=None,
        situations=None,
        sitRefs=None,
        attributes=None,     
        sevCriticalCount=0,
        sevWarningCount=0,
        sevInfoCount=0,
        sevOtherCount=0, 
    ):
        ITMFolder.system_count += 1
        self.folderId = folderId
        self.supported = getSupportedString(supported)
        self.description = description
        self.situations = situations       
        self.sitRefs = sitRefs
        self.attributes = attributes
        self.sevCriticalCount=sevCriticalCount,
        self.sevWarningCount=sevWarningCount,
        self.sevInfoCount=sevInfoCount,
        self.sevOtherCount=sevOtherCount, 
        
    # def toJSON(self):
    #     return json.dumps(self, default=lambda o: o.__dict__, 
    #        indent=4, ensure_ascii=False)
    
    
            
    def getITMFolderReportArray(self):
        return [            
            self.folderId,
            self.supported,
            self.description,
            self.situations, 
            self.sitRefs
        ]
    
    def update(self, ITMFolder):
        print(self.toString)
        print(ITMFolder.toString)
        
    def toString(self):
        return self.folderId + ":" + self.agents

    def report(self, file):
        file.write("\n==================================================\n\n")
        file.write("  " + self.folderId + " - " + self.description)
        file.write("\n==================================================\n\n")
        file.write("Situation count  : " + str(len(self.situations)) + "\n")
        file.write("Ref (UNTIL) count: " + str(len(self.sitRefs)) + "\n")
        file.write("\n\n")
        file.write("Maintenance Window\n")
        file.write("---------------\n\n")

def getSupportedString(self):
    if isNaN(self):
        return "NOT_SUPPORTED"
    elif self is None:
        return "UNKNOWN"
    else:
        return self

def isNaN(num):
        return num != num
    
# CONSTANTS
#  Excel Report Column Headers
systemHeaders = ["Folder", "SituationsitRefs", "Situations"]