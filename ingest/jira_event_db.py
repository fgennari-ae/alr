from event_db import EventDb
import jira as JIRA

class JiraEventDb(EventDb):

    def __init__(self):
        self.jira_api_key = None

    def _create_session(self, session_id):
        if session_id:
            return 'Jira-session-key'
        return None

    def _create_event_in_session(self, session_id, event):
        #check session exists
        if event:
            if session_id:
                #create event in event db
                return 'Jira-event-key'
        return None

    def session_exists(self, session_id):
        return True
    
    def connect(self):
        try:
            #attempt to connect to Jira
            return True
        except Exception as e:
            print("Unsuccessful attempt of connection to Jira")
            return False


