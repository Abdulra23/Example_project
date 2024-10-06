import os, logging, string, secrets
from decouple import config
from constants import (TOOL_NAME,TOOL_VERSION)
import pandas as pd

def getPathWithSeparator(path):
    if path.endswith(os.sep):
        return path
    else:
        return path + os.sep

def read_seed_data(filePath) -> pd.DataFrame:
    seed_data_df = pd.read_csv(filePath)
    return seed_data_df

def get_newid(key_char_length):
    alphabet = string.ascii_letters + string.digits
    password = ''.join(secrets.choice(alphabet) for i in range(key_char_length))
    return password

# Load environment variables
MIGRATION_PATH = getPathWithSeparator(config("MIGRATION_FOLDER", default="migration"))
NAME = config("MIGRATION_NAME", default="ITM2Instana").replace(" ", "_")
JSON_DESTINATION=config('JSON_DESTINATION')
ALERT_JSON_DESTINATION=config('ALERT_JSON_DESTINATION')

# setup migration tool output folder paths
logPath = MIGRATION_PATH + "logs" + os.sep + NAME + os.sep
os.makedirs(logPath, exist_ok=True)
os.makedirs(JSON_DESTINATION, exist_ok=True)
os.makedirs(ALERT_JSON_DESTINATION, exist_ok=True)

# setup logger
logLevel = config("LOG_LEVEL", default="INFO") 
logFormat = "%(asctime)s|%(levelname)s|%(message)s" 
if logLevel == "REPORT":
    logFormat = "%(message)s"
    logLevel = "INFO"
logging.basicConfig(
    filename=logPath + "analysis.log", level=logLevel, format=logFormat
)
logger = logging.getLogger()
logger.info("\n" + TOOL_NAME + " " + TOOL_VERSION)

# Load Migration Tool Seed Data
SEED_DATA_PATH = config("MIGRATION_FOLDER") + os.sep + "seed_data" + os.sep
itm2instana_mappings_df = read_seed_data(SEED_DATA_PATH + "itm2instanaMapping.csv")
metric_mappings_df = read_seed_data(SEED_DATA_PATH + "metricMapping.csv")
product_codes_df = read_seed_data(SEED_DATA_PATH + "ITMProductCodes.csv")
