from enum import Enum

TOOL_NAME = "ITM2Instana Migration Tool"
TOOL_VERSION = "v1.0"

attribute_string_operators = [
    "*EQ",
    "*GT",
    "*NE",
    "*GE",
    "*LT",
    "*LE",
    "*YES",
    "*NO",
]

attribute_string_keywords = ["*VALUE", "*STR", "*SCAN"]

# example PDTs
#  *IF *MISSING Process.Base_Command *EQ ('qdaemon')
#  *IF *SCAN SNMP-MANAGERMANAGED-NODES00.Node_Status *EQ Off-line
#  *IF *VALUE ManagedSystem.Status *EQ '*OFFLINE' 
system_event_rule = ["*MISSING", "*MS_OFFLINE", "Off-line", "OFFLINE"]

another_situation_reference = "*SIT"

pdt_ignore = ["*IF", "*OR", "*AND", " ", "(", ")"]

maint_window = "maintenanceWindow"

INSTANA_SEV_CRITICAL = "10"
INSTANA_SEV_WARNING = "5"
INSTANA_SEV_INFO = "0"

SUPPORTED = "SUPPORTED"
NOT_SUPPORTED = "NOT_SUPPORTED"
CUSTOM = "CUSTOM"
UNKNOWN = "UNKNOWN"
WORKAROUND = "WORKAROUND"