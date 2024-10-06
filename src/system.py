class ManagedSystem:
    system_count = 0
 
    def __init__(
        self,
        hostName=None,
        agents=None,
        situations=None,
        groups=None,
        situationCount=0,
        migrateYes=0,
        migrateNo=0,
        roadmap=0,
        action=0
    ):
        ManagedSystem.system_count += 1
        self.hostName = hostName
        self.agents = agents
        self.situations = situations       
        self.groups = groups  
        self.situationCount = situationCount
        self.migrateYes = migrateYes
        self.migrateNo = migrateNo
        self.roadmap = roadmap
        self.action = action
        
    

    def getManagedSystemReportArray(self):
        print("System " + self.hostName + " has " + str(len(self.agents)) + " agents installed on it.")
        print("is a member of " + str(len(self.groups)) + " groups, and associated with " + str(len(self.situations)) + " situations.")
        # return {        
        #     self.hostName,
        #     self.agents,  
        #     self.situations, 
        #     self.groups,
        #     f'{self.mappings}'
        # }
    
    def update(self, managedSystem):
        print(self.toString)
        print(managedSystem.toString)
        
    def toString(self):
        return self.hostName + ":" + self.agents

# CONSTANTS
#  Excel Report Column Headers
systemHeaders = ["System", "Mappings", "Agent #", "Agents", "MSL #", "MSLs", "Situations #", "Situations"]