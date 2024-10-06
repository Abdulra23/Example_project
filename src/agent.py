class Agent:
    system_count = 0
 
    def __init__(
        self,
        productCode=None,
        name=None,
        supported=None,
        attributeSupportedCount=0,
        attributeSupportedCountPercentage=0,
        attributeRoadmapCount=0,
        attributeNotSupportedCount=0,
        attributeOtherCount=0,
        attributeTotalCount=0,
        comments=None
    ):
        Agent.system_count += 1
        self.productCode = productCode
        self.name = name
        self.supported = supported
        self.attributeSupportedCount = attributeSupportedCount
        self.attributeSupportedCountPercentage = attributeSupportedCountPercentage
        self.attributeRoadmapCount = attributeRoadmapCount
        self.attributeNotSupportedCount = attributeNotSupportedCount
        self.attributeOtherCount = attributeOtherCount
        self.attributeTotalCount = attributeTotalCount
        self.comments = comments

        
    
    
