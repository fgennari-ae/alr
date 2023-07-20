from datatypes import Session, Event
from google_event_db import GoogleEventDb
from aws_helper import AwsHelper
from datetime import datetime
import boto3
import os, shutil
import logging
import sys

# Setting Logger

log_filename = datetime.now().strftime('EventDataIngest_%H_%M_%d_%m_%Y.log')
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
logging.basicConfig(level=logging.DEBUG,
                    handlers=[file_handler, console_handler])
logger = logging.getLogger('EventDataIngest')

class EventDataIngest:
   
    def __init__(self, aws_bucket_name, event_db, temp_folder):
        self.new_sessions = {}
        self._clear_working_folder(temp_folder)
        self.event_db = event_db
        self.aws = AwsHelper(bucket_name = aws_bucket_name,
                             local_download_folder = temp_folder)
        
    def _clear_working_folder(self, folder):
        logger.info('Clearing temporary working folder')
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.warn('Failed to delete %s. Reason: %s' % (file_path, e))
        logger.info("Temporary folder is now empty")

    def _get_sessions_in_timeframe(self, timeframe):
        #get session metadata from aws and creates a session object with related events
        sessions = {}
        all_sessions_paths = self.aws.get_paths_to_available_sessions(timeframe=timeframe)
        for session_path in all_sessions_paths:
            session = self.aws.create_session_from_path(session_path)
            if session:
                sessions[session.session_id]=session
        return sessions


    def print_new_sessions(self):
        print("List of all the identified sessions with annotations:")
        if not self.new_sessions:
            print("No new sessions to upload")
        for session_id in self.new_sessions:
            print([self.new_sessions[session_id].session_id, 
                   ', Number of events: ', 
                   len(self.new_sessions[session_id].events)])

    def check_connections(self):
        #check connection to aws
        if not self.aws.connect():
            return False
        #check connection to EventDb
        if not self.event_db.connect():
            return False
        return True

    def retrieve_sessions(self, timeframe): 
        logger.info("Checking AWS for new sessions")
        #retrieve list of new sessions paths (check only completed uploads)
        all_new_sessions = self._get_sessions_in_timeframe(timeframe=timeframe)
        
        for session_id in all_new_sessions:
            if not self.event_db.session_exists(session_id):
                #store the new session for upload
                logger.info("Found a new session: " + session_id)
                self.new_sessions[session_id] = all_new_sessions[session_id]
        if self.new_sessions:
            for session_id in self.new_sessions:
                #download the audio tags locally for the new session
                self.aws.get_audio_tags_from_session_locally(self.new_sessions[session_id]) 
            return
        logger.info("No New Session to Upload!")
        return

    def upload_new_sessions(self):
        for session_id in self.new_sessions: 
            if not self.event_db.session_exists(session_id):
                if not self.new_sessions[session_id].skip:
                    self.event_db.upload(self.new_sessions[session_id])


