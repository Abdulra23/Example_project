

def get_sitInfo_attribute(sit_info, lookup_attribute, start_trim_by='<![CDATA[', key_value_splitter='=', attribute_splitter=';'):
    ''' Given a string like sit_info , find the lookup value in the sub attribute
    '''
    if sit_info:
        if (not sit_info.find(key_value_splitter)) or len(sit_info) < 5:
            return None
        # remove starting `<![CDATA[` from sit_info
        if sit_info.startswith(start_trim_by):
            sit_info = sit_info[sit_info.find(start_trim_by)+len(start_trim_by):]
        sit_info = sit_info.strip("[]<!>;~")  # remove leading/trailing garbage
        lookup_attribute = lookup_attribute.upper()
        # converting string to dict <key:value> pair
        sit_attribs_dict = dict(attribute.split(key_value_splitter) for attribute in sit_info.split(attribute_splitter))
        # return value of the attribute if attribute exists in dictionary, otherwise return None
        if sit_attribs_dict.get(lookup_attribute):
            return sit_attribs_dict[lookup_attribute]
        else:
            return None
    else:
        return None        



class Situation:
    situation_count = 0

    def __init__(self, SITNAME= None, FULLNAME= None, TEXT= None, AFFINITIES= None, PDT= None, 
                REEV_DAYS = None,REEV_TIME= None, AUTOSTART= None, ADVISE= None, CMD= None, AUTOSOPT= None,
                DISTRIBUTION= None,ALERTLIST= None,HUB= None,QIBSCOPE= None,SENDMSGQ= None,DESTNODE= None,
                LOCFLAG= None,LSTCCSID= None, LSTDATE= None,LSTRELEASE= None,LSTUSRPRF= None,NOTIFYARGS= None,
                NOTIFYOPTS= None,OBJECTLOCK= None,PRNAMES= None,REFLEXOK= None, SITINFO= None, SOURCE=None, MAP=None):
        Situation.situation_count+=1
        self.autostart = AUTOSTART
        self.distribution = DISTRIBUTION
        self.sit_name = SITNAME
        self.full_name = FULLNAME
        self.text = TEXT
        self.affinities = AFFINITIES
        self.reev_days = REEV_DAYS
        self.reev_time = REEV_TIME
        self.advise = ADVISE
        self.cmd = CMD
        self.autosopt = AUTOSOPT
        self.alert_list = ALERTLIST
        self.hub = HUB
        self.qib_scope = QIBSCOPE
        self.send_msg_q = SENDMSGQ
        self.dest_node = DESTNODE
        self.loc_flag = LOCFLAG
        self.lst_cc_sid = LSTCCSID
        self.lst_date = LSTDATE
        self.lst_release = LSTRELEASE
        self.lst_usr_prf = LSTUSRPRF
        self.notify_args= NOTIFYARGS
        self.notify_opts = NOTIFYOPTS
        self.object_lock = OBJECTLOCK
        self.pr_names = PRNAMES
        self.reflex_ok = REFLEXOK
        self.sit_info = SITINFO
        self.sit_info_count = get_sitInfo_attribute(sit_info=self.sit_info, lookup_attribute='COUNT')
        self.severity = get_sitInfo_attribute(sit_info=self.sit_info, lookup_attribute='SEV')
        self.source = SOURCE
        # retrieve values that needs a conversion (1-M mapping)
        self.pdt = PDT
        self.map = MAP

    def get_name(self):
        try:
            if self.full_name:
                return self.full_name
            elif self.sit_name:
                return self.sit_name
            else:
                return None
        except:
            return None
            
    def get_sit_name(self):
        return self.sit_name
        
    def get_autostart(self):
        autostart = self.autostart.strip("*")
        return True if autostart.upper() == "YES" else False
    
    def get_distribution(self):
        return self.distribution if self.distribution else None

    def get_pdt(self):
        return self.pdt if self.pdt else None
        
    def get_severity(self):
        return self.severity if self.severity else None

    def get_sit_info_count(self):
        return int(self.sit_info_count) if self.sit_info_count else int(1)

    def get_reev_time(self):
        return self.reev_time if self.reev_time else None

    def get_text(self):
        return self.text if self.text else None

    def get_map(self):
        return self.map if self.map else None