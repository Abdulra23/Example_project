import os, json, re, math, xmltodict
import pandas as pd
import preprocessor, upload, alerts
from situation import Situation
from decouple import config
from shared import logger
from ast import literal_eval

# read necessary env variables
JSON_DESTINATION=config('JSON_DESTINATION')
TRANSFORM_PREFIX=config('TRANSFORM_PREFIX')
PROMPT_UPLOAD=config('PROMPT_UPLOAD')


def get_description_from_situation_code(situation_code)-> str:
    desc = None
    try:
        desc = preprocessor.situation_descriptions_df[preprocessor.situation_descriptions_df['situation_code']==situation_code]['situation_description'].item()    
    except:
        desc = None
        logger.error("Situation cannot be found in seed data i.e. sit_desc.csv file")
    return desc

def get_entity_type_from_situation_code(situation_code)-> str:
    entityType = None
    try:
        # first get the situation type from AllSituationByType seed data
        situationType = preprocessor.all_sit_by_type_seed_data_df[preprocessor.all_sit_by_type_seed_data_df['situation_name']==situation_code]['situation_type'].item()
        # then, find the relevant entity type against that situation type (itm_prod_category) from the sitProd2EntityType (instana_entityType)
        entityType = preprocessor.sitProd2EntityType_df[preprocessor.sitProd2EntityType_df['itm_prod_category']==situationType]['instana_entityType'].item()    
    except:
        logger.error("Entity name cannot be found in seed data i.e. sitProd2EntityType.csv file")
    return entityType

def get_metricMappings_for_ITM_attributegroup_and_attributeItem(attribute_group, attribute_item):
    metric_mapping = None
    try:
        metric_mapping = preprocessor.metric_mappings_df[(preprocessor.metric_mappings_df['ITM_Attribute_Group']==attribute_group) & (preprocessor.metric_mappings_df['ITM_Attribute_Item']==attribute_item)]
    except:
        logger.error("Metric Mappings not found in seed data i.e. metricMapping.csv")
    return metric_mapping

def get_query_filter_from_ITM_Product_code(ITM_product_code):
    query = None
    try:
        query = preprocessor.ITM_product_codes_df[(preprocessor.ITM_product_codes_df['ProductCode']==ITM_product_code)]['DFQ'].item()
    except:
        logger.error("No ITM Product codes found in seed data i.e. ITMProductCodes.csv")
    return query if query else None

def get_keyword_mapping(itm_keyword):
    keyword_mapping = None
    itm_keyword = itm_keyword.strip() if itm_keyword else itm_keyword
    try:
        # TODO: pass dynamic columns names from dataframe rather than hard coded values
        keyword_mapping = preprocessor.itm2instana_mappings_df[preprocessor.itm2instana_mappings_df['ITM_XML']==itm_keyword]['Instana_JSON_value'].item()
    except:
        logger.error(itm_keyword + " Keyword Mappings not found in seed data i.e. itm2instanaMapping.csv")
        keyword_mapping = None
    return keyword_mapping

def apply_operand_conversion_rule(operand, conversion_rule) -> str:
    if conversion_rule:
        if conversion_rule == "percentage":
            return float(operand) / 100
        elif conversion_rule == "enumConversion":
            #get the values from ITM2InstanaMapping.csv seed data file
            conversionValue = get_keyword_mapping(operand)
            return conversionValue if conversionValue else operand
        elif conversion_rule == "time":
            #handle time interval conversion here, convert ITM seconds to millicesonds for instana
            return operand*1000
        elif conversion_rule == "MB2byte":
            # 1 MB = 1024*1024 bytes = 1048576
            return float(operand)/1048576
        elif conversion_rule == "KB2byte":
            return float(operand)/1024
        elif conversion_rule == "value":
            #TODO:  how to handle count, for now just returning the value as is
            return operand     
        elif conversion_rule == "dynamicQuery":
            logger.info("Populating a dynamic query") # No further action needed
            return operand
    else:
        return operand

def convert_reeve_time_into_milliseconds(reeve_time):
    hours = reeve_time[0:2]
    minutes = reeve_time[2:4]
    seconds = reeve_time[4:]
    total_seconds = ((int(hours)*3600) + (int(minutes)*60) + int(seconds) ) *1000
    return total_seconds

def round_time_values(time_to_round, is_grace_period=False):
    #return 1s if less than 1000 milliseconds
    if time_to_round <= 1000:
        return 1000
    # return the closer of 1s or 5s 
    if time_to_round > 1000 and time_to_round <=5000:
        return 1000 if time_to_round - 2500 <= 0 else 5000
    # return the closer of 5s or 10s 
    if time_to_round > 5000 and time_to_round <=10000:
        return 5000 if time_to_round - 7500 <= 0 else 10000
    # return the closer of 10s or 30s 
    if time_to_round > 10000 and time_to_round <=30000:
        return 10000 if time_to_round - 20000 <= 0 else 30000
    # return the closer of 30s or 60s 
    if time_to_round > 30000 and time_to_round <=60000:
        return 30000 if time_to_round - 45000 <= 0 else 60000
    # return the closer of 60s or 90s 
    if time_to_round > 60000 and time_to_round <=90000:
        return 60000 if time_to_round - 75000 <= 0 else 90000
    # return the closer of 90s or 5m 
    if time_to_round > 90000 and time_to_round <=300000:
        return 90000 if time_to_round - 195000 <= 0 else 300000
    # return the closer of 5m or 10m 
    if time_to_round > 300000 and time_to_round <=600000:
        return 300000 if time_to_round - 450000 <= 0 else 600000
    # return the closer of 10m or 30m 
    if time_to_round > 600000 and time_to_round <=1800000:
        return 600000 if time_to_round - 1200000 <= 0 else 1800000
    # return the closer of 30m or 60m 
    if time_to_round > 1800000 and time_to_round <=3600000:
        return 1800000 if time_to_round - 2700000 <= 0 else 3600000
    # return the closer of 60m or 90m 
    if time_to_round > 3600000 and time_to_round <=5400000:
        return 3600000 if time_to_round - 4500000 <= 0 else 5400000
    # return the closer of 90m or 120m 
    if time_to_round > 5400000 and time_to_round <=7200000:
        return 5400000 if time_to_round - 6300000 <= 0 else 7200000
    # return the closer of 120m or 4h 
    if time_to_round > 7200000 and time_to_round <=14400000:
        return 7200000 if time_to_round - 10800000 <= 0 else 14400000
    
    # only check following values in case of grace period
    if is_grace_period:
        # return the closer of 4h or 6h 
        if time_to_round > 14400000 and time_to_round <=21600000:
            return 14400000 if time_to_round - 18000000 <= 0 else 21600000
        # return the closer of 6h or 12h 
        if time_to_round > 21600000 and time_to_round <=43200000:
            return 21600000 if time_to_round - 32400000 <= 0 else 43200000
        # return the closer of 12h or 24h 
        if time_to_round > 43200000 and time_to_round <=86400000:
            return 43200000 if time_to_round - 64800000 <= 0 else 86400000
        else: # return the max value of 86400000
            return 86400000
    else: # returning the highest possible value for metric time window
        return 14400000

def convert_pdt_formula_to_rules_list(pdt_formula, severity,metric_time_window):
    logger.info(pdt_formula)
    attribute_string_keywords = ['*VALUE','*STR','*SCAN']
    attribute_string_operators = ['*EQ','*GT','*NE','*GE','*LT','*LE','*YES','*NO']
    system_event_rule = ['*MISSING','*MS_OFFLINE']
    another_situation_reference = '*SIT'
    time_to_live_keyword = '*TTL'
    local_time_keyword = 'Local_Time'
    pdt_ignore = ['*IF','*OR','*AND', ' ', '(',')']
    

    if severity:
        severity = int(get_keyword_mapping(severity)) if get_keyword_mapping(severity) and severity else int(5)
    else:
        severity = int(5)
        

    #TODO: set the default severity to 0-informational once availbale in instana. Setting to 5-warning as default for now.
    #get severity from seed data lookup
    if severity==0:
        severity=int(5)

    rules = []         # rules object to be added to the current event
    status = None
    query = None
    entity_type = None
    grace_period_milliseconds = None
    rules_for_event_copy = None   # rules to add in a copy of the current event e-g, due to *OR keyword
    

    pdt_array = pdt_formula.split()
    if another_situation_reference in pdt_array:
        logger.warning("The situation referencing another situation not allowed. "+ pdt_formula)
        return rules, rules_for_event_copy, query, entity_type, status, grace_period_milliseconds
    # check if pdt formula items are found in system event rule array
    is_system_rule = [True for item in pdt_array if item in system_event_rule]
    
    if is_system_rule:
        # create a system event rule
        # TODO: wrap this in a check and create a system rule instead of normal rule
        logger.warning("This is a system rule event situation:  "+pdt_formula)
        status = 'System Event Rule'

    # Calculate the Grace Period from *UNTIL *TTL keywords from PDT
    if time_to_live_keyword in pdt_array:
        ttl_value = pdt_array[pdt_array.index(time_to_live_keyword)+1] # get value of *TTL
        if ttl_value:
            ttl_array = ttl_value.split(':')
            # The format of the *TTL 0:00:15:00 is d:hh:mm:ss
            grace_period_milliseconds = ((int(ttl_array[0])*86400) + (int(ttl_array[1])*3600) + (int(ttl_array[2])*60) + int(ttl_array[3])) * 1000
            logger.info("Grace Period in milliseconds:  "+ str(grace_period_milliseconds))
    
    def isNaN(num):
        return num !=num

    def get_attribute_value_from_df(attribute_df, attribute_name):
        attribute_value = None
        try:
            # set to None, if record is present but value is empty in dataframe
            if attribute_df[attribute_name].item() and not isNaN(attribute_df[attribute_name].item()):
                attribute_value = attribute_df[attribute_name].item() 
        except:
            attribute_value = None
        return attribute_value

    def get_values_for_single_condition(list_items_in_single_pdt_condition):
        logger.info("Fetching metric mapping values for a single condition.")
        iterator = 0
        count =0
        rule_metric_mappings_df = None #todo: unset and find the root cause of failure
        condition_operator = None
        operand = None
        query_value = None
        for iterator in range(len(list_items_in_single_pdt_condition)):
            item = list_items_in_single_pdt_condition[iterator]
            #*IF *VALUE Queue_Statistics.Queue_Name *EQ Q289P.PROD.SA.SAST.REQUEST *AND *VALUE Queue_Statistics.Current_Depth *GE 1000
            #
            if item == '*IF' or item in attribute_string_keywords:
                keyword_string = item
                continue  #skipping

            # find attribute_group.attribute_Item, then split and retrieve metric mappings from seed data
            if '.' in item and (list_items_in_single_pdt_condition[iterator-1] in attribute_string_keywords):
                items = item.split('.')
                rule_metric_mappings_df = get_metricMappings_for_ITM_attributegroup_and_attributeItem(attribute_group=items[0], attribute_item=items[1])
            # check if there is an *EQ keyword, fetch its mapping value from seed data
            elif item in attribute_string_operators:
                condition_operator = get_keyword_mapping(item)
            elif item and list_items_in_single_pdt_condition[iterator-1] in attribute_string_operators:
                operand = item.strip('\'"')

            iterator+=1
        
        entity_type = get_attribute_value_from_df(rule_metric_mappings_df, 'entityType')
        metric_name = get_attribute_value_from_df(rule_metric_mappings_df,'metricName')
        # fetch aggregation applicable, set `avg` by default if None
        aggregation = get_attribute_value_from_df(rule_metric_mappings_df,'aggregation')
        aggregation=aggregation if aggregation else 'avg'
        query_value = get_attribute_value_from_df(rule_metric_mappings_df,'query')
        metric_pattern_prefix = get_attribute_value_from_df(rule_metric_mappings_df,'metricPattern.prefix')
        metric_pattern_postfix = get_attribute_value_from_df(rule_metric_mappings_df,'metricPattern.postfix')
        # check if an operand conversion rule is applicable
        conversion_rule = get_attribute_value_from_df(rule_metric_mappings_df, 'conversionRule')
        # apply operand value conversion rules if required
        operand = apply_operand_conversion_rule(operand, conversion_rule) if conversion_rule else operand

        return entity_type, metric_name, metric_pattern_prefix, metric_pattern_postfix, aggregation, query_value, condition_operator, operand

    def handle_single_condition(condition, query_operator, query, rules, entity_type, metric_pattern_rule = None):
        isMetricPattern = True if metric_pattern_rule else False

        # check if Local_Time metric was used in the condition, then return
        if condition.find(local_time_keyword) != -1:
            return query, rules, entity_type, metric_pattern_rule

        # split pdt formula based on spaces ' '
        list_items_in_single_pdt_condition = condition.split()

        # check if condition is an *UNTIL *TTL condition, return as there won't be any rule added
        if time_to_live_keyword in list_items_in_single_pdt_condition:
            return query, rules, entity_type, metric_pattern_rule

        rule_entity_type, metric_name, metric_pattern_prefix, metric_pattern_postfix, aggregation, query_value, condition_operator, operand = get_values_for_single_condition(list_items_in_single_pdt_condition)
        if entity_type is None:
            entity_type = rule_entity_type

        if rule_entity_type is not None and metric_name is None and metric_pattern_prefix is None and metric_pattern_postfix is None and query_value is not None:
            if query is None:
                query = query_value +':\''+ operand+'\''
            elif query is not None:
                query = query + query_operator + query_value +':\''+ operand+'\''
        elif rule_entity_type is not None and metric_name is not None:
            # TODO: create a rules class
            is_duplicate_metric_rule = [True for r in rules if r['metricName'] == metric_name]
            if is_duplicate_metric_rule:
                logger.warning("A duplicate metricName was found in PDT formula. Skipped adding a new rule into rules array.")
            else: 
                rule = {
                            "ruleType": "threshold",
                            "metricName": metric_name,
                            "metricPattern": None,
                            "rollup": 0,
                            "window": metric_time_window,
                            "aggregation": aggregation if aggregation else None,
                            "conditionOperator": condition_operator if condition_operator else None,
                            "conditionValue": operand if operand else None,
                            "severity": severity
                        }
                rules.append(rule)
        # handle dynamic metric patterns, its always in pair of two conditions
        elif metric_name is None and query_value is None: 
            # metric name and query is always null in the seed data metricMappings,
            #  in case of dynamic metric pattern conditions in the PDT formula
            
            # if no metric pattern exists, means this is first condition of dynamic metric pattern
            if not isMetricPattern: 
                
                metric_pattern_rule = {
                            "ruleType": "threshold",
                            "metricName": None,
                            "metricPattern": {
                                "prefix": metric_pattern_prefix if metric_pattern_prefix else None,
                                "postfix": metric_pattern_postfix if metric_pattern_postfix else None,
                                "placeholder": None if aggregation else operand,
                                "operator": "is"
                            },
                            "rollup": 0,
                            "window": metric_time_window,
                            "aggregation": aggregation if aggregation else None,
                            "conditionOperator": condition_operator if condition_operator else None,
                            "conditionValue": operand if operand else None,
                            "severity": severity
                        }
            elif isMetricPattern:
                # insert left over variable values in the rule from second condition
                metric_pattern_dict = metric_pattern_rule.pop('metricPattern')
                metric_pattern_dict['prefix'] = metric_pattern_prefix if metric_pattern_dict['prefix'] is None else metric_pattern_dict['prefix']
                metric_pattern_dict['postfix'] = metric_pattern_postfix if metric_pattern_dict['postfix'] is None else metric_pattern_dict['postfix']
                metric_pattern_dict['placeholder'] = operand if metric_pattern_dict['placeholder'] is None else metric_pattern_dict['placeholder']
                # merge the metricPattern object back to parent dictionary object
                metric_pattern_rule['metricPattern'] = metric_pattern_dict

                metric_pattern_rule['aggregation'] = aggregation if metric_pattern_rule['aggregation'] is None else metric_pattern_rule['aggregation'] 
                metric_pattern_rule['conditionOperator'] = condition_operator if metric_pattern_rule['conditionOperator'] is None else metric_pattern_rule['conditionOperator']
                metric_pattern_rule['conditionValue'] = operand if metric_pattern_rule['conditionValue'] is None else metric_pattern_rule['conditionValue']
                
                rules.append(metric_pattern_rule)
                metric_pattern_rule = None
                isMetricPattern = False

        # logger.info("query" + str(query))
        # logger.info(" entity_type" + str(entity_type))
        return query, rules, entity_type, metric_pattern_rule
                
    def handle_conditions(pdt_formula, query, rules, entity_type, query_operator = ' OR ', metric_pattern_rule=None):
        if pdt_formula not in pdt_ignore and pdt_formula:
            pdt_sub_array = pdt_formula.split()
            if '(' in pdt_sub_array or ')' in pdt_sub_array:
                logger.info("handling brackets, splitting ")
                pdt_regex_array = re.split(r"\(|\)", pdt_formula)
                for item in pdt_regex_array:
                    query, rules, entity_type, metric_pattern_rule = handle_conditions(item.strip(), query, rules, entity_type, query_operator, metric_pattern_rule)
            elif '*OR' in  pdt_sub_array:
                query_operator = ' OR '
                logger.info("handling *ORs")
                pdt_ors = pdt_formula.split('*OR')
                for item in pdt_ors:
                    query, rules, entity_type, metric_pattern_rule = handle_conditions(item.strip(), query, rules, entity_type, query_operator, metric_pattern_rule)            
            elif '*AND' in pdt_sub_array:
                query_operator = ' AND '
                logger.info("Handling *ANDs")
                pdt_ands = pdt_formula.split('*AND')
                for item in pdt_ands:
                    query, rules, entity_type, metric_pattern_rule = handle_conditions(item.strip(), query, rules, entity_type, query_operator, metric_pattern_rule) 
            else:
                logger.info("handling single condition()"+ pdt_formula)
                query, rules, entity_type, metric_pattern_rule = handle_single_condition(pdt_formula, query_operator, query, rules, entity_type, metric_pattern_rule)
        return query, rules, entity_type, metric_pattern_rule

    query, rules, entity_type, metric_pattern_rule = handle_conditions(pdt_formula,query, rules, entity_type, query_operator=' OR ')
    #else: if a normal rule
        # TODO: create a normal event rule
        # split pdt formula based on ANDs conditions if present

    return rules, rules_for_event_copy, query, entity_type, status, grace_period_milliseconds

def transform_distribution_to_dynamic_focus_query(distribution):
    # split based on space, then split  based on : if found. Then if two values found with :
    #  then just add entity.tag:  else get dfq value from the seed data ITM codes files
    # and insert midle part in the entity.tag and first part with df_value
    dfq = ' '
    if distribution:
    # todo: ignore the * in the distribution. 
    # add split funct with space and , 

        #dist_items = distribution.split(',')
        dist_items = re.split('\\s|,', distribution)
        iterator = 0
        for iterator in range(len(dist_items)):
        #for dist_value in dist_items:
            dist_value = dist_items[iterator]
            # skipp if value starting from * by convention
            if dist_value.find('*') == -1:
                # check if ":" is present in the distribution text
                if dist_value.find(':') != -1:
                    managed_systems_items = dist_value.split(':')
                    if len(managed_systems_items) == 2:
                        dfq += 'entity.tag:\''+managed_systems_items[0]+'\''
                    elif len(managed_systems_items) == 3:
                        dfq_agent_query_filter = get_query_filter_from_ITM_Product_code(managed_systems_items[2])
                        dfq += 'entity.tag:\''+managed_systems_items[1]+'\''
                        if dfq_agent_query_filter:
                            dfq += ' OR '+ dfq_agent_query_filter +':\''+managed_systems_items[0]+'\''
                else:
                    dfq += 'entity.tag:\''+dist_value+'\''
                if dfq and dfq != ' ' and iterator < len(dist_items)-1:
                    dfq+= ' OR '
            iterator+=1
    return dfq if dfq else None


def create_json_events_from_situations(sits_list=list[Situation]):
    # Instana will generate a new ID when the event is created
    for situation in sits_list:
        sit_name = situation.get_name()
        get_sit_name_for_id = situation.get_sit_name()
        pdt_formula = situation.get_pdt()
        sit_text = situation.get_text()
        description = sit_text if sit_text else get_description_from_situation_code(sit_name)
        distribution = situation.get_distribution()
        logger.info("Distribution:   " + str(distribution))
        dfq_value = None
        if distribution:
            dfq_value = transform_distribution_to_dynamic_focus_query(distribution)
            logger.info('DFQ: '+dfq_value)
        enabled = False
        grace_period = 0
        autostart = situation.get_autostart()
        # calculate metric time window from reeve_time and sit_info_count values
        reeve_time = situation.get_reev_time()
        sit_info_count = situation.get_sit_info_count()
        reeve_time_in_milliseconds = convert_reeve_time_into_milliseconds(reeve_time)
        metric_time_window_raw = int(reeve_time_in_milliseconds) * int(sit_info_count)
        metric_time_window_rounded = round_time_values(metric_time_window_raw, is_grace_period = False)
        
        # check for all required attributes before creating json
        if sit_name and pdt_formula and autostart:
            rules, rules_for_event_copy, query, entity_type, status, grace_period_milliseconds = convert_pdt_formula_to_rules_list(pdt_formula=pdt_formula, severity=situation.get_severity(), metric_time_window=metric_time_window_rounded)
            
            # Hanlde grace period 
            #If no "UNTIL" clause found in the PDT then default value for Grace Period should be double whatever we used for the time window.
            if grace_period_milliseconds:
                grace_period = round_time_values(grace_period_milliseconds, is_grace_period = True)
            else:
                grace_period = round_time_values(metric_time_window_rounded * 2, is_grace_period=True)

            # AND query with distribution value if both exists, or if query exists (may be with distribution added as query)
            # set enabled to True if query exists
            if query and dfq_value:
                enabled = True
                query = query+' OR '+dfq_value
            elif query is None and dfq_value:
                enabled = True
                query = dfq_value
            elif query and dfq_value is None:
                enabled = True

            # add distribution data to query

            eventJSON = {        
            "id": get_sit_name_for_id,
            "name": TRANSFORM_PREFIX+sit_name,
            "entityType": entity_type,
            "query": query,
            "triggering": False,
            "description": description,
            "expirationTime": grace_period,
            "enabled": enabled,
            "rules": rules
            }
            # use four indents to make it easier to read the result:
            jsonString = json.dumps(eventJSON, indent=4)
            with open(JSON_DESTINATION+sit_name+".json", "w") as jsonFile:
                jsonFile.write(jsonString)
            
            # add EIF slots if map attribute is set with custom payload for alerting
            map = situation.get_map()
            if map:
                custom_payload_fields = preprocessor.convert_map_string_to_custom_payload_fields(map)
                # invoke the json alert creation with alert name and EIF data
                alerts.create_alert(get_sit_name_for_id+"_alert", events_list=[get_sit_name_for_id],alert_channels_list=[],custom_payload_fields=custom_payload_fields)
        else:
            logger.error("Situation not active. Skiped! "+ sit_name)

