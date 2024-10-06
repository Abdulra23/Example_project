import unittest
import pandas as pd
from situation import Situation, get_sitInfo_attribute
from preprocessor import populate_seed_data, xml_files_reader, read_sitProd2EntityType_seed_data ,XML_to_CSV_converter_getDescription, read_situation_descriptions_seed_data, get_entity_type_from_situation_code, get_description_from_situation_code


class TestMigration(unittest.TestCase):
    def test_get_sitInfo_attribute(self):
        self.assertEqual(get_sitInfo_attribute('SEV=Informational','SEV',start_trim_by='<![CDATA['), 'Informational')
        self.assertEqual(get_sitInfo_attribute('<![CDATA[SEV=Critical]]>','SEV',start_trim_by='<![CDATA['), 'Critical')
        self.assertEqual(get_sitInfo_attribute('TFWD=Y;SEV=Informational;TDST=1;~;','SEV',start_trim_by='<![CDATA['), 'Informational')
        self.assertEqual(get_sitInfo_attribute('SEV=Critical;~','SEV',start_trim_by='<![CDATA['), 'Critical')
        self.assertEqual(get_sitInfo_attribute('COUNT=72;TFWD=Y;SEV=Fatal;TDST=1;~;','SEV',start_trim_by='<![CDATA['), 'Fatal')
        self.assertEqual(get_sitInfo_attribute('TFWD=Y;SEV=Minor;TDST=1;~;','SEV',start_trim_by='<![CDATA['), 'Minor')
        self.assertEqual(get_sitInfo_attribute('[SEV=Unknown;~','SEV',start_trim_by='<![CDATA['), 'Unknown')
        self.assertEqual(get_sitInfo_attribute('SEV=Warning;~','SEV',start_trim_by='<![CDATA['), 'Warning')
        self.assertEqual(get_sitInfo_attribute('SEV=Harmless;~','SEV',start_trim_by='<![CDATA[', key_value_splitter='=', attribute_splitter= ';'), 'Harmless')
        self.assertEqual(get_sitInfo_attribute('<![CDATA[ATOM=KV6FNHELTH.FABRICI_ID;SEV=Critical;TFWD=Y;TDST=0]]>','SEV',start_trim_by='<![CDATA['), 'Critical')
        self.assertEqual(get_sitInfo_attribute(' ','SEV',start_trim_by='<![CDATA['), None)
        self.assertEqual(get_sitInfo_attribute('=','SEV',start_trim_by='<![CDATA['), None)

    def test_read_sitProd2EntityType_seed_data(self):
        self.assertIsNotNone(read_sitProd2EntityType_seed_data('data/seed_data/sitProd2entityType.csv'))
        self.assertEqual(type(read_sitProd2EntityType_seed_data('data/seed_data/sitProd2entityType.csv')), pd.DataFrame)

    def test_XML_to_CSV_converter_getDescription(self):
        # TODO: extend the functionality and then add unit tests
        self.assertEqual(type(XML_to_CSV_converter_getDescription('data/asf',columns=['situation_code', 'situation_description'], index=['situation_code'])), int)
        
    def test_read_situation_descriptions_seed_data(self):
        self.assertIsNotNone(read_situation_descriptions_seed_data('data/seed_data/allSituationsByType.txt'))
        self.assertIs(type(read_situation_descriptions_seed_data('data/seed_data/allSituationsByType.txt')), pd.DataFrame)

    # def test_XML_reader(self):
    #     self.assertEqual(type(XML_reader('data/xml')), int)

    def test_get_description_from_situation_code(self):
        # arrange
        populate_seed_data()
        # act & assert
        self.assertIsNone(get_description_from_situation_code('MQSeries_Appl_MQ_RespTime_High'))
        self.assertIsNone(get_description_from_situation_code('KBN_DPC_CPU_High'))

    
    def test_get_entity_type_from_situation_code(self):
        # arrange
        #populate_seed_data() # donot need this again.
        # act & assert
        self.assertEqual(get_entity_type_from_situation_code('MQSeries_Appl_MQ_RespTime_High'), 'ibmMqQueue')
    
    def test_xml_files_reader(self):
        #arrange 
        self.assertNotEqual(len(xml_files_reader('data/xml/sub-folder')),0)
        self.assertEqual(type(xml_files_reader('data/xml/sub-folder')), list)


    # Test functions for class Situations
    def test_situation_get_name(self):
        # arrange
        sit_name="sit_name"
        full_name="full_name"
        sit = Situation(SITNAME=sit_name, FULLNAME=full_name)

        # act
        name_returned = sit.get_name()
        # assert 
        self.assertEqual(name_returned, full_name)

        sit = Situation(SITNAME=sit_name, FULLNAME=None)
        # act
        name_returned = sit.get_name()
        # assert 
        self.assertEqual(name_returned, sit_name)

        sit = Situation(SITNAME=None, FULLNAME=None)
        # act
        name_returned = sit.get_name()
        # assert 
        self.assertEqual(name_returned, None)

    def test_situation_get_autostart(self):
        # arrange 
        sit = Situation(AUTOSTART='*NO')
        # act and assert
        self.assertFalse(sit.get_autostart())

        sit = Situation(AUTOSTART='*YES', DISTRIBUTION=' ')
        # act and assert
        self.assertTrue(sit.get_autostart())


if __name__=='__main__':
	unittest.main()