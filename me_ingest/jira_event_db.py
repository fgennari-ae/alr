from event_db import EventDb
from jira import JIRA

class JiraEventDb(EventDb):

    def __init__(self):
        self.jira_api_key = 'lWVSmBuv5c6llE5qWaqJ8G4RmUER3cCxohoNSH'
        self.jira = None 
        self.fmap = {'name':               'customfield_14003',
                     'vehicle_id':         'customfield_49805',
                     'city':               'customfield_20007',
                     'sds_version':        'customfield_52102',
                     'map_version':        'customfield_52103',
                     'driver_information': 'customfield_52101',
                     'severity':           'customfield_11701'}
        
        result = True

    def _create_session(self, session_id):
        #Mock to get events created (no session ticket gets currently created)
        if session_id:
            return session_id
        return None

    def _create_event_in_session(self, session_id, event):
        #check session exists
        if event:
            if session_id:
                #create event in event db
                return 'Jira-event-key'
        return None

    def session_exists(self, session_id):
        if self.jira.search_issues(jql_str='Summary ~ ' + session_id):
            return True
        return False
        
    
    def connect(self):
        try:
            self.jira = JIRA(server="https://devstack.vwgroup.com/jira/", 
                             token_auth=self.jira_api_key)
            if self.jira.myself():
                print("Successful attempt of connection to Jira")
                return True
            else:
                return False
        except Exception as e:
            print("Unsuccessful attempt of connection to Jira")
            print(e)
            return False


