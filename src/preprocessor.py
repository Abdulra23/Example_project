import os, json, xmltodict
import pandas as pd
from situation import Situation
import transformer
from shared import logger



def convert_map_string_to_custom_payload_fields(map_value)->list:
    custom_payload_fields = []
    try:
        map_dict = xmltodict.parse(map_value)
        logger.info("Adding EIF slot's custom pay load values")
        slots=map_dict['situation']['slot']
        slot_keys=['msg','AdviceCode','NotifyAction','NotifyCategory','NotifyEmailGroup']
        for slot in slots:
            slot_name=slot['@slotName']
            if slot_name in slot_keys:
                literal_string_value=slot['literalString']['@value']
                custom_payload_fields.append({'key':slot_name,'type':'staticString','value':literal_string_value})
    except Exception as e:
        logger.error(e)
        custom_payload_fields=None
    return custom_payload_fields


def XML_to_CSV_converter_getDescription(inputDirectoryPath,columns, index=None)->int:
    # NOTE: this method is used once in this process: to build a look up catalog on sit_name & sit_desc
    # retruns number of files processed

    situation_description = ''
    sit_desc_start = '  <!-- Situation Description: '
    sit_desc_end = '-->'
    sit_code_start_1 = '    <SITUATION>'
    sit_code_start_2 = '<SITUATION NAME="'
    sit_code_end_2 = '" INTERVAL'

    keywords_dict = {'situation_code':'situation_description'}
    df = pd.DataFrame(columns=columns, index=None)
    iterator = 0

    if os.path.isdir(inputDirectoryPath) and os.path.exists(inputDirectoryPath):
        for folder, subs, files in os.walk(inputDirectoryPath):
            for file in files:
                if file.endswith('.xml'):
                    with open(os.path.join(folder, file), 'r') as xmlFile:                    
                        while True:
                            line = xmlFile.readline()
                            if not line:
                                break
                            if line.startswith(sit_desc_start) or line.startswith(sit_desc_start.strip()):
                                # situation_description = line[len(sit_desc_start):-5] # remove ending tag
                                situation_description = line[line.find(sit_desc_start)+len(sit_desc_start) -1:line.rfind(sit_desc_end)]

                            elif line.startswith(sit_code_start_1):
                                situation_code = line[len(sit_code_start_1):-13] # remove ending tag
                                keywords_dict[situation_code] = situation_description
                                df.at[ iterator, columns[0]] = situation_code
                                df.at[ iterator, columns[1]] = situation_description
                                iterator +=1
                            elif line.startswith(sit_code_start_2):
                                situation_code = line[len(sit_code_start_2):line.find(sit_code_end_2)]
                                keywords_dict[situation_code] = situation_description
                                df.at[ iterator, columns[0]] = situation_code
                                df.at[ iterator, columns[1]] = situation_description
                                iterator +=1

    else:
        logger.error("Not a directory. A valid directory path is required to process data.")
        return None

    df.to_csv('data/seed_data/sit_desc.csv',index=False, mode='w')
    return iterator

def create_profiling_on_pdt_formula_to_attributeGroup_MetricItem(sits_list):
    attribute_item_list =[]
    attribute_string_keywords = ['*VALUE','*STR','*SCAN']

    for sit in sits_list:
        pdt_formula = sit.get_pdt()
        # split pdt formula based on spaces ' '
        list_items = pdt_formula.split()
        iterator = 0
        count =0
        for iterator in range(len(list_items)):
            item = list_items[iterator]

            if item == '*if':
                continue  #skipping
            # check if there is an *EQ keyword, fetch its mapping value from seed data
            if item == '*EQ' or item == '*GT' or item == '*NE' or item == '*GE' or item == '*LT' or item == '*LE' or item == '*YES' or item == '*NO':
                condition_operator = transformer.get_keyword_mapping(item)
            # find attribute_group.attribute_Item, then split and retrieve metric mappings from seed data
            if '.' in item and (list_items[iterator-1] in attribute_string_keywords):
                if item not in attribute_item_list:
                    attribute_item_list.append(item)
                items = item.split('.')
                rule_metric_mappings_df = transformer.get_metricMappings_for_ITM_attributegroup_and_attributeItem(attribute_group=items[0], attribute_item=items[1])
            if item in '*AND':
                count+=1

            iterator+=1
    return attribute_item_list

def read_all_situations_by_type_seed_data(filePath,columns)-> pd.DataFrame:
    with open(filePath,'r') as situations_file_reader:
        df = pd.DataFrame(columns=columns, index=None)
        # read the header info in the header of the file i.e. column names
        line = situations_file_reader.readline()

        iterator = 0
        
        while True:
            line = situations_file_reader.readline()
            tuple_text = line.partition(' ')
            name = tuple_text[0].strip()
            type = tuple_text[2].strip()
            df.at[iterator, columns[0]]= name
            df.at[iterator, columns[1]]= type
            iterator +=1

            if not line:
                break
        # Examples to access data from dataframe
        # prints a series of all situations for type 'DB2' (get multiple values)
        #print (df[df['situation_type'] == 'DB2']['situation_name'])
        # prints a single lookup value for situation type where situation name is 'UDB_Appl_Wait_Lock_2' (get a single value)
        #print(df[df['situation_name']=='UDB_Appl_Wait_Lock_2']['situation_type'].item())
        return df

def read_sitProd2EntityType_seed_data(filePath)-> pd.DataFrame:
    sitProd2EntityType_df = pd.read_csv(filePath)
    # print(sitProd2EntityType_df)
    # # Examples to access data from dataframe
    # itm_prod_category = 'itm_prod_category'
    # db2= 'DB2'
    # instana_entityType ='instana_entityType'
    # # prints a single lookup value for instana_entityType where itm_prod_categoryis 'DB2' (get a single value)
    # print(sitProd2EntityType_df[sitProd2EntityType_df[itm_prod_category]==db2][instana_entityType].item())
    return sitProd2EntityType_df

def read_instana_metrics_seed_data(inputDirectoryPath, columns)-> pd.DataFrame:
    main_metrics_df = pd.DataFrame(columns=columns, index=None)
    main_metrics_df['custom']= main_metrics_df['custom'].astype(bool)
    if os.path.isdir(inputDirectoryPath) and os.path.exists(inputDirectoryPath):
        for folder, subs, files in os.walk(inputDirectoryPath):
                for file in files:
                    if file.endswith('.json'):
                        with open(os.path.join(folder, file), 'r') as jsonFile:
                            instana_metric_catalog_df = pd.read_json(jsonFile)
                            instana_metric_catalog_df['custom']=instana_metric_catalog_df['custom'].astype(bool)
                        main_metrics_df = pd.concat([main_metrics_df,instana_metric_catalog_df])
    else:
        print("Please provide a valid directory path.")
        return None
    #print(main_metrics_df[main_metrics_df['pluginId']=='host']['label'])
    return main_metrics_df

def read_situation_descriptions_seed_data(filePath)-> pd.DataFrame:
    sit_desc_df = pd.read_csv(filePath)
    return sit_desc_df

def read_itm2instanaMapping_seed_data(filePath)-> pd.DataFrame:
    itm2instana_mapping_df = pd.read_csv(filePath)
    return itm2instana_mapping_df

def read_metricMapping_seed_data(filePath)-> pd.DataFrame:
    metric_mapping_df = pd.read_csv(filePath)
    return metric_mapping_df

def read_ITMProductCodes_seed_data(filePath)-> pd.DataFrame:
    ITM_product_codes = pd.read_csv(filePath)
    return ITM_product_codes

def get_description_from_situation_code(situation_code)-> str:
    desc = None
    try:
        desc = situation_descriptions_df[situation_descriptions_df['situation_code']==situation_code]['situation_description'].item()    
    except:
        desc = None
        logger.warning("Situation cannot be found in seed data i.e. sit_desc.csv file")
    return desc

def get_entity_type_from_situation_code(situation_code)-> str:
    entityType = None
    try:
        # first get the situation type from AllSituationByType seed data
        situationType = all_sit_by_type_seed_data_df[all_sit_by_type_seed_data_df['situation_name']==situation_code]['situation_type'].item()
        # then, find the relevant entity type against that situation type (itm_prod_category) from the sitProd2EntityType (instana_entityType)
        entityType = sitProd2EntityType_df[sitProd2EntityType_df['itm_prod_category']==situationType]['instana_entityType'].item()    
    except:
        logger.warning("Entity name cannot be found in seed data i.e. sitProd2EntityType.csv file")
    return entityType

def populate_seed_data():
    ''' Read all seed data needed to build catalog for migration
    '''
    global instana_matrix_df
    global sitProd2EntityType_df
    global all_sit_by_type_seed_data_df
    global situation_descriptions_df
    global itm2instana_mappings_df
    global metric_mappings_df
    global ITM_product_codes_df
    try:
        instana_matrix_df = read_instana_metrics_seed_data('../data/seed_data/instana_metrics', columns=['formatter','label','description','metricId','pluginId','custom'])
        sitProd2EntityType_df = read_sitProd2EntityType_seed_data('../data/seed_data/sitProd2EntityType.csv')
        all_sit_by_type_seed_data_df = read_all_situations_by_type_seed_data('../data/seed_data/allSituationsByType.txt', columns=['situation_name', 'situation_type'])
        situation_descriptions_df = read_situation_descriptions_seed_data('../data/seed_data/sit_desc.csv')
        itm2instana_mappings_df = read_itm2instanaMapping_seed_data('../data/seed_data/itm2instanaMapping.csv')
        metric_mappings_df = read_metricMapping_seed_data('../data/seed_data/metricMapping.csv')
        ITM_product_codes_df = read_ITMProductCodes_seed_data('../data/seed_data/ITMProductCodes.csv')
    except Exception as e:
        logger.error("Error in loading seed data:\t "+ str(e))

def xml_files_reader(inputDirectoryPath)-> list[Situation]:
    # convert an XML file format data in to equivalent situation class objects
    situations_list = []
    if os.path.isdir(inputDirectoryPath) and os.path.exists(inputDirectoryPath):
            for folder, subs, files in os.walk(inputDirectoryPath):
                    for file in files:
                        if file.endswith('.xml'):
                            with open(os.path.join(folder, file), 'r') as xmlFile:
                                try:
                                    ordered_data_dict = xmltodict.parse(xmlFile.read())
                                    # check if the there are multiple ROW attributes e-g:- data with EIF slots
                                    if ordered_data_dict['TABLE']['ROW']:
                                        row_data_dict = dict(ordered_data_dict['TABLE']['ROW'])
                                        sit = convert_row_dict_to_situation_class_instance(row_dict=row_data_dict)
                                        situations_list.append(sit)
                                except Exception as e:
                                    try:
                                        if ordered_data_dict['TABLE']['ROW'][0]:
                                            row_data_dict = dict(ordered_data_dict['TABLE']['ROW'][0])
                                            sit = convert_row_dict_to_situation_class_instance(row_dict=row_data_dict)                                            
                                            if ordered_data_dict['TABLE']['ROW'][1]:
                                                row_data_dict2 = dict(ordered_data_dict['TABLE']['ROW'][1])
                                                sit2 = convert_row_dict_to_situation_class_instance(row_dict=row_data_dict2)
                                                sit.map = sit2.map
                                            situations_list.append(sit)  
                                    except Exception as e:
                                            logger.error('Error parsing file.  '+ file +' \t'+ str(e))
    else:
        logger.error("Please provide a valid directory path.")
        return None    
    logger.info("Preprocessing finished with total situation count = \t"+str(len(situations_list)))
    return situations_list

def convert_row_dict_to_situation_class_instance(row_dict)-> Situation:
    logger.info('processing converting row dict to situation class instance')
    def get_attribute_value_if_exists(row_dict, attribute_name):
        try:
            if row_dict[attribute_name]:
                return row_dict[attribute_name]
        except:
            # print("Missing attribute found:", attribute_name)
            return None
            
    situation = Situation(
        SITNAME = get_attribute_value_if_exists(row_dict,'SITNAME'),
        FULLNAME = get_attribute_value_if_exists(row_dict,'FULLNAME'),
        TEXT = get_attribute_value_if_exists(row_dict,'TEXT'),
        AFFINITIES = get_attribute_value_if_exists(row_dict,'AFFINITIES'), 
        PDT = get_attribute_value_if_exists(row_dict,'PDT'), 
        REEV_DAYS  = get_attribute_value_if_exists(row_dict,'REEV_DAYS'),
        REEV_TIME = get_attribute_value_if_exists(row_dict,'REEV_TIME'), 
        AUTOSTART = get_attribute_value_if_exists(row_dict,'AUTOSTART'), 
        ADVISE = get_attribute_value_if_exists(row_dict,'ADVISE'),
        CMD = get_attribute_value_if_exists(row_dict,'CMD'), 
        AUTOSOPT = get_attribute_value_if_exists(row_dict,'AUTOSOPT'),
        DISTRIBUTION = get_attribute_value_if_exists(row_dict,'DISTRIBUTION'),
        ALERTLIST = get_attribute_value_if_exists(row_dict,'ALERTLIST'),
        HUB = get_attribute_value_if_exists(row_dict,'HUB'),
        QIBSCOPE = get_attribute_value_if_exists(row_dict,'QIBSCOPE'),
        SENDMSGQ = get_attribute_value_if_exists(row_dict,'SENDMSGQ'),
        DESTNODE = get_attribute_value_if_exists(row_dict,'DESTNODE'),
        LOCFLAG = get_attribute_value_if_exists(row_dict,'LOCFLAG'),
        LSTCCSID = get_attribute_value_if_exists(row_dict,'LSTCCSID'),
        LSTDATE = get_attribute_value_if_exists(row_dict,'LSTDATE'),
        LSTRELEASE = get_attribute_value_if_exists(row_dict,'LSTRELEASE'),
        LSTUSRPRF = get_attribute_value_if_exists(row_dict,'LSTUSRPRF'),
        NOTIFYARGS = get_attribute_value_if_exists(row_dict,'NOTIFYARGS'),
        NOTIFYOPTS = get_attribute_value_if_exists(row_dict,'NOTIFYOPTS'),
        OBJECTLOCK = get_attribute_value_if_exists(row_dict,'OBJECTLOCK'),
        PRNAMES = get_attribute_value_if_exists(row_dict,'PRNAMES'),
        REFLEXOK = get_attribute_value_if_exists(row_dict,'REFLEXOK'),
        SITINFO = get_attribute_value_if_exists(row_dict,'SITINFO'), 
        SOURCE  = get_attribute_value_if_exists(row_dict,'SOURCE'),
        MAP  = get_attribute_value_if_exists(row_dict,'MAP')
    )
    return situation