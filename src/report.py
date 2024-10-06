class Report:
    report_count = 0

    def __init__(
        self,
        folderIdSupported=None,
        supported=None,
        notSupported=None,
        todo=None,
        missing=None,
        folderId=None,
        agent=None,
        total=0,
        sitName=None,
        active=None,
        severity=None,
        sevCritical=None,
        sevWarning=None,
        sevInfo=None,
        sevOther=None,
        interval=None,
        command=None,
        hasChange=None,
        hasCustomAdvise=None,
        advise=None,
        description=None,
        pdt=None,
        hasEIF=None,
        hasRange=None,
        hasMetricPattern=None,
        hasDistribution=None,
        distribution=None,
        pdtCount=None,
        hasDelta=None,
        hasHistRule=None,
        hasPercentChange=None,
        hasUntilSit = None,
        hasSitRef=None,
        hasMaintWindow=None,
        hasMapping=None,
        isSystem=None,
        map=None,
        distCount=0,
        orCount=0,
        andCount=0,
        mappingScore=0
    ):
        Report.report_count += 1
        self.supported = supported,
        self.folderId = folderId
        self.folderIdSupported = folderIdSupported,        
        self.agent = agent
        self.total = total
        self.sitName = sitName
        self.active = active
        self.severity = severity
        self.sevInfo = sevInfo
        self.sevOther = sevOther
        self.sevCritical = sevCritical
        self.sevWarning = sevWarning
        self.interval = interval
        self.command = command
        self.hasChange = hasChange
        self.hasCustomAdvise = hasCustomAdvise
        self.advise = advise
        self.description = description
        self.hasEIF = hasEIF
        self.hasDistribution = hasDistribution
        self.distribution = distribution
        self.pdtCount = pdtCount
        self.hasRange = hasRange
        self.hasMetricPattern = hasMetricPattern
        self.hasSitRef = hasSitRef
        self.isSystem = isSystem
        self.hasMaintWindow = hasMaintWindow
        self.hasMapping = hasMapping
        self.hasDelta = hasDelta        
        self.hasHistRule = hasHistRule
        self.hasPercentChange = hasPercentChange
        self.hasUntilSit = hasUntilSit
        self.pdt = pdt
        self.map = map
        self.distCount = distCount
        self.orCount = orCount
        self.andCount = andCount
        self.notSupported = notSupported
        self.todo = todo
        self.missing = missing
        self.mappingScore = mappingScore
    
    def getSummaryReportArray(self):
        return [            
            self.agent,
            self.folderId,  
            self.folderIdSupported,               
            convertVal(self.total),
            convertVal(self.active),
            convertVal(self.total - self.active),
            convertVal(self.mappingScore),
            convertVal(self.isSystem),
            convertVal(self.hasMaintWindow),
            convertVal(self.hasEIF),
            convertVal(self.hasDistribution),            
            convertVal(self.command),
            convertVal(self.advise),
            convertVal(self.pdtCount),
            convertVal(self.hasChange),
            convertVal(self.hasMapping),
            convertVal(self.hasRange),
            convertVal(self.hasMetricPattern),
            convertVal(self.hasDelta),
            convertVal(self.hasHistRule),
            convertVal(self.hasPercentChange), 
            convertVal(self.hasUntilSit),    
            convertVal(self.sevInfo),
            convertVal(self.sevCritical),
            convertVal(self.sevWarning),
            convertVal(self.sevOther),   
            convertVal(self.supported),
            convertVal(self.notSupported),
            convertVal(self.todo),
            convertVal(self.missing)
            
        ]

    def getSituationReportArray(self):
        return [ 
            self.folderId,        
            self.sitName,            
            self.andCount,
            self.orCount,
            self.mappingScore,
            self.isSystem,
            self.hasMaintWindow,
            self.severity,
            self.interval,
            self.command,
            self.hasCustomAdvise,
            self.advise,
            self.pdtCount,
            self.hasChange,
            self.hasEIF != None,
            self.hasDistribution != None and self.hasDistribution != "*NONE",     
            self.distCount,       
            self.hasMapping,
            self.hasRange,
            self.hasMetricPattern,
            self.hasDelta,
            self.hasHistRule,
            self.hasPercentChange,   
            self.hasUntilSit,    
            self.hasSitRef,     
            self.description,
            self.pdt,  
            self.map,      
            self.distribution,            
            self.supported,
            self.notSupported,
            self.todo,
            self.missing,            
        ]
    
    def printReport(self):
        print("*************** REPORT ***************")
        print("Folder   : " + str(convertVal(self.folderId)))
        # print("Supported: " + self.folderIdSupported)        
        # print("Supported: " + convertVal2(self.folderIdSupported))
        # print("Agent    : " + self.agent)

    def isFolderSupported(self):
        return convertVal2(self.folderIdSupported)
    
def convertVal2(val):
    # ignore None and (None,) and (nan,)    
    if isNaN(val[0]):        
        return "UNKNOWN"   
    elif val != None and not len(val) == val.count(None):
        return val[0]
    else:
        return val
    
def isNaN(num):
    return num != num
  
def convertVal(val):
    # ignore None and (None,)
    # isNone = len(val) == val.count(None)
    if val != None and isinstance(val, int):
        return int(val)
    else:
        return val

# CONSTANTS
#  Excel Report Column Headers
summaryHeaders = [
    "Agent",
    "Folder", 
    "Instana Sensor",     
    "Situations",
    "Active (to be migrated)",
    "Inactive (skipped)",
    "Score Today",    
    "System Event",
    "Maint Window",
    "hasEIF",
    "hasDistribution",
    "hasCommand",
    "hasCustomAdvice",
    "PDTCount",
    "hasCHANGE",
    "hasMapping",
    "hasRange",
    "hasMetricPattern",
    "DELTA",
    "HISTRULE",
    "PCTCHANGE",
    "Until Sit",
    "SEV:Info",
    "SEV:Critical",
    "SEV:Warning",
    "SEV:Other",
    "Supported",
    "NOT Supported",
    "ToDo",
    "Missing"
    
]

reportHeaders = [
    "Folder",    
    "Name",
    "AND",
    "OR",       
    "Score Today",
    "System Event",
    "Maint Window",
    "Severity",
    "Interval",
    "Command",
    "hasCustomAdvice",
    "Advice",
    "PDTCount",
    "hasCHANGE",
    "hasEIF",
    "hasDistribution",
    "DistributionCount", 
    "hasMapping",
    "hasRange",
    "hasMetricPattern",
    "DELTA",
    "HISTRULE",
    "PCTCHANGE",
    "Until Sit",
    "hasSitRef",
    "Description",
    "PDT",
    "MAP",  
    "DISTRIBUTION",      
    "Supported",
    "NOT Supported",
    "ToDo",
    "Missing",
    
]