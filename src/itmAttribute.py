class ITMAttribute:
    system_count = 0
 
    def __init__(
        self,
        attrValue=None,
        attrItem=None,
        supported=None,
        comment=None
    ):
        ITMAttribute.system_count += 1
        self.attrValue = attrValue
        self.attrItem = attrItem
        self.supported = supported     
        self.comment = comment
          
        
    
    def getITMAttributeReportArray(self):
        return {        
            self.attrValue,
            self.attrItem,  
            self.supported
        }
            
    def toString(self):
        return self.attrValue + ":" + self.attrItem

