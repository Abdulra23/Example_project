class SituationChecklist:
    system_count = 0
 
    def __init__(
        self,
        sitName=None,
        folderId=None,
        autoStart=None,
        severity=None,
        pdt=None,
        pdtMappings=None,
        distribution=None,
        isRef=None,
        untilRef=None,
        migrate=None,
        itmSituation=None
    ):
        SituationChecklist.system_count += 1
        self.sitName = sitName
        self.folderId = folderId
        self.autoStart = autoStart
        self.severity = severity
        self.migrate = migrate   
        self.isRef = isRef
        self.untilRef = untilRef    
        self.pdt = pdt
        self.pdtMappings = pdtMappings
        self.distribution = distribution
        self.itmSituation = itmSituation
        
    
    def getChecklistReportArray(self):
        return {        
            self.sitName,
            self.severity,  
            self.migrate
        }
            
    def toString(self):
        return self.sitName + ":" + self.severity

