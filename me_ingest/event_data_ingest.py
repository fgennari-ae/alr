from datatypes import Session, Event
from google_event_db import GoogleEventDb
from mysql_event_db import MySQLEventDb
from aws_helper import AwsHelper
from datetime import datetime
from tabulate import tabulate
import boto3
import os, shutil
import logging
import sys

# Setting Logger

log_filename = datetime.now().strftime('logs/EventDataIngest-%Y_%m_%d_%H_%M.log')
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
logging.basicConfig(level=logging.DEBUG,
                    handlers=[file_handler, console_handler])
logger = logging.getLogger('EventDataIngest')

class EventDataIngest:
     """
    A class used to represent the Ingest Job, it provides an interface to all the tasks that needs to be performed to get, manipulate and store events from fleet testing.

    ...

    Attributes
    ----------
    new_sessions : str
        An empty dictionary where the key is the session id that needs to be processed and as value the Session object
    event_db : EventDb
        The database class used to store event data
    aws = AwsHelper
        An instance of the helper to get data and metadata from the aws bucket

    Methods
    -------
    print_report()
        Prints the report for the processed sessions

    check_connections()
        Checks the connection to the remote sources (aws, EventDb)

    retrieve_sessions(timeframe)
        retrieves new sessions in timeframe. As of now this is checking existing sessions and will need to switch to single events
        TODO: move to single events check.

    upload_new_sessions()
        uploads the events from new sessions to the database of Events provided

    """
  
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


    def print_report(self):
        total_skipped_events = 0
        total_processed_events = 0
        empty_table_data = []
        skipped_table_data = []
        processed_table_data = []
        for sid in self.new_sessions:
            if self.new_sessions[sid].skip:
                total_skipped_events += len(self.new_sessions[sid].events)
                if self.new_sessions[sid].skip_reason != "No Events":
                    #adding sessions to skipped table data
                    if len(self.new_sessions[sid].events) > 0:
                        num_events = len(self.new_sessions[sid].events)
                    else:
                        num_events = "Unknown"
                    skipped_table_data.append([sid,
                                               self.new_sessions[sid].country,
                                               self.new_sessions[sid].skip,
                                               self.new_sessions[sid].skip_reason,
                                               num_events])
                elif self.new_sessions[sid].skip_reason == "No Events":
                    #adding sessions to empty table data
                    empty_table_data.append([sid,
                                             self.new_sessions[sid].country,
                                             self.new_sessions[sid].skip])
            else:
                total_processed_events += len(self.new_sessions[sid].events)
                #adding sessions to processed table data
                processed_table_data.append([sid,
                                             self.new_sessions[sid].country,
                                             self.new_sessions[sid].skip, 
                                             len(self.new_sessions[sid].events)])

        empty_table = tabulate(empty_table_data, 
                headers=['Session', 'Country', 'Skipped'], 
                                tablefmt='orgtbl')
        skipped_table = tabulate(skipped_table_data, 
                headers=['Session', 'Country', 'Skipped', 'Reason', 'Number of Events'], 
                                tablefmt='orgtbl')
        processed_table = tabulate(processed_table_data, 
                headers=['Session', 'Country', 'Skipped', 'Number of Events'], 
                                tablefmt='orgtbl')
        summary_table = tabulate([['Skipped Events', total_skipped_events],
                                  ['Processed Events', total_processed_events],
                                  ["----", ""],
                                  ['Total', total_skipped_events + total_processed_events]],
                                 headers=['Description', 'Value'], 
                                 tablefmt='orgtbl')
        print("Sessions with no events in metadata file")
        print(empty_table)
        print(" ")
        print("Sessions Skipped")
        print(skipped_table)
        print(" ")
        print("Sessions Processed")
        print(processed_table)
        print(" ")
        print("Sessions Processed - Summary")
        print(summary_table)
        print(" ")
        logger.debug("Sessions with no Events:\n" + empty_table)
        logger.debug("Sessions with Events unable to process:\n" + skipped_table)
        logger.debug("Processed Sessions:\n" + processed_table)
        logger.debug("Summary:\n" + summary_table)

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
                #download the audio tags locally for the new sessions with events
                if self.new_sessions[session_id].events:
                    self.aws.get_audio_tags_from_session_locally(self.new_sessions[session_id])
                else:
                    #skipping sessions with no events  (still getting them for the report)
                    if not self.new_sessions[session_id].skip:
                        self.new_sessions[session_id].skip=True
                        self.new_sessions[session_id].skip_reason="No Events"
            return
        logger.info("No New Session to Upload!")
        return

    def upload_new_sessions(self):
        for session_id in self.new_sessions: 
            if not self.event_db.session_exists(session_id):
                if not self.new_sessions[session_id].skip:
                    self.event_db.upload(self.new_sessions[session_id])


