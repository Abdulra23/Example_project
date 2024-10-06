import preprocessor, transformer, time, upload
from decouple import config
from shared import logger

start_time = time.time()

# read necessary env variables
XML_INPUT_DATA_FOLDER = config('XML_INPUT_DATA_FOLDER')
PROMPT_UPLOAD=config('PROMPT_UPLOAD')

logger.info("Preprocessing started.")
preprocessor.populate_seed_data()

sits_list = preprocessor.xml_files_reader(XML_INPUT_DATA_FOLDER) # TODO: File paths: use string constants or read from env file. 

logger.info("Transformation started.")
transformer.create_json_events_from_situations(sits_list=sits_list)
preprocessor.create_profiling_on_pdt_formula_to_attributeGroup_MetricItem(sits_list=sits_list)

if PROMPT_UPLOAD=="True":
    logger.info("Upload json events started.")
    uploaded_files_count=upload.JSON_import()
    logger.info(str(uploaded_files_count) +"files have been uploaded successfully" )

logger.info("Total execution time:  "+str(time.time() -start_time))

# Uncomment to create profiling for pdt formula
# attribute_item_list =preprocessor.create_profiling_on_pdt_formula_to_attributeGroup_MetricItem(sits_list)
# with open('data/seed_data/attributeGroup_items.txt','w') as tfile:
# 	tfile.write('\n'.join(attribute_item_list))

# print(get_description_from_situation_code('MQSeries_Appl_MQ_RespTime_High'))
# print(get_description_from_situation_code('KBN_DPC_CPU_High'))

# print(get_entity_type_from_situation_code('MQSeries_Appl_MQ_RespTime_High'))
# print(get_entity_type_from_situation_code('KBN_DPC_CPU_High'))

# XML_reader('data/xml') #not used anymore.
# XML_to_CSV_converter_getDescription('data/asf',columns=['situation_code', 'situation_description'], index=['situation_code'])

#*IF ( ( *VALUE KLZ_Disk.Mount_Point *EQ '/aebsu02/app' *AND ( ( ( *VALUE KLZ_Disk.Disk_Used_Percent *GT 95 *AND *VALUE KLZ_Disk.System_Name *EQ 'xfinapm3p:LZ' ) *OR ( *VALUE KLZ_Disk.Disk_Used_Percent *GT 96 *AND ( ( ( *VALUE KLZ_Disk.System_Name *EQ 'xfinapm4p:LZ' ) *OR ( *VALUE KLZ_Disk.System_Name *EQ 'xfinapw3p:LZ' ) ) ) ) ) ) ) *OR ( *VALUE KLZ_Disk.Mount_Point *EQ '/aetnas43/oracmprod' *AND *VALUE KLZ_Disk.Disk_Used_Percent *GT 96 *AND *VALUE KLZ_Disk.System_Name *EQ 'xfinapm4p:LZ' ) )