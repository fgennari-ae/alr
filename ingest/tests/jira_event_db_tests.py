import sys
sys.path.append('..')

from jira_event_db import JiraEventDb
from datatypes import Event
import unittest
import warnings

class TestJiraEventDb(unittest.TestCase):

    def test_connection(self):
        db = JiraEventDb()
        self.assertTrue(db.connect())
    
    def test_session_exists(self):
        db = JiraEventDb()
        db.connect()
        self.assertTrue(db.session_exists('AV11_V237_230714_130715_62100'))
    
    def test_session_does_not_exists(self):
        db = JiraEventDb()
        db.connect()
        self.assertFalse(db.session_exists('AV11_V239_210714_130715_62100'))

if __name__ == '__main__':
    unittest.main()
