# ITM-to-Instana-migration
Scripts and resources to move resources like Threshold to Instana

## Pre-requisits:
 - Install [python](https://www.python.org/downloads/). Just FYI this project was built on python v3.9.10
 - Review the [guidelines](/docx/migration_tool_guide.md) for working with migration tool.
 - Just install python for now :D  


## Create a virtual env to run this application
 1. Create a `venv` to run this application so that it may not ammend your current `python3` environments.

 ```
  python3 -m venv /path/to/new/virtual/environment
 ```
 > Hint: You can just replace `/path/to/new/virtual/environment` with `env` and python will create a local env in your current directory.

 1. Activate the `venv` you just created (`<venv>` must be replaced by the path of the directory containing the virtual environment):
 
 ```
  source <venv>/bin/activate
 ```

 1. Insatll the dependencies with the command below

 ```
  pip install -r requirements.txt
 ```

 1. Make sure your current directory is `src` and then run the main.py file with command below
 ```
 python main.py
 ```

 ## .env
 The following environment variables are needed to run the Analysis report
 ```
MIGRATION_NAME="ClientName"
MIGRATION_FOLDER="../data"
ITM_FOLDER="/Bulk/SITUATION"
LOG_LEVEL="DEBUG"
XML_INPUT_DATA_FOLDER="../data/input/xml"
JSON_FOLDER="../data/json/events/"
JSON_DESTINATION="../data/json/events/"
ALERT_JSON_DESTINATION="../data/json/alerts/"
API_TOKEN="****************"
TRANSFORM_PREFIX="ITM2Instana_"
BASE_URL="https://muhammad-abubaker-vm.fyre.ibm.com"
ITM_SYSLIST="path to your Managed systems list"
PROMPT_UPLOAD="True"
```