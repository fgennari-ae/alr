from oauth2client.service_account import ServiceAccountCredentials
from botocore.client import Config
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import speech_recognition as sr
from event_db import EventDb
from datetime import datetime
from pytz import timezone

import boto3
import requests
import json
import pymysql
import logging
import gspread
import os

logger = logging.getLogger('MySQLEventDb')

class MySQLEventDb(EventDb):

    def __init__(self, database):
        self.database = database
        self.sessions_in_db = []
        self.sql_connection = None
        self.s3_client = None
    
    def _create_session(self, session_id):
        fake_session_key = dict()
        fake_session_key['id']=session_id
        return fake_session_key 

    def _create_event_in_session(self, session_key, event):
        if not self.s3_client:
            logger.warn("S3 Client not set up")
            return False
        try:
            logger.debug("Creating file on s3 for event " + event.audio_tag)
            s3_new_key = session_key['id'] + "/" + event.audio_tag
            self.s3_client.upload_file(Filename=event.local_path, Bucket='alr-event-data', Key=s3_new_key)
        except Exception as e:
            logger.debug("There was an error uploading the file")
            logger.error(e)
            return False
        comment_from_audio = 'ND'
        with sr.AudioFile(event.local_path) as source:
            logger.debug("Trying to transcript " + event.audio_tag)
            audio = self.transcriber.record(source)  # read the entire audio file                  
            try:
                event.comment = str(self.transcriber.recognize_google(audio, language = 'en-IN'))
            except:
                logger.debug("An exception occurred while transcribing the audio, ignoring")
                pass
            #add event info 
        event.file_url=self.s3_client.generate_presigned_url('get_object', 
                                                                  Params = {'Bucket' : 'alr-event-data', 
                                                                            'Key' : s3_new_key}, 
                                                                  ExpiresIn = 600000)
        creation_date = datetime.now(timezone('Europe/Berlin')).strftime('%Y-%m-%d %H:%M:%S')
        logger.debug("Saving event in MySQL Database")
        cursor = self.sql_connection.cursor()
        sql = "INSERT INTO sds (vehicle,\
                                country,\
                                city,\
                                timestamp,\
                                comment,\
                                annotation,\
                                sds_provider_id,\
                                sw_release,\
                                session_id,\
                                map_version,\
                                driver,\
                                codriver,\
                                log_slice_link,\
                                mission,\
                                mission_segment,\
                                creation_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (event.vehicle_id,          
                             event.country,      
                             event.city,               
                             event.timestamp,
                             event.comment,
                             event.annotation,
                             event.audio_tag,
                             event.sw_release,
                             event.session_id,
                             event.map_version,
                             event.driver,
                             event.codriver,                                                                                                  
                             event.file_url,                                        
                             event.mission,                               
                             "ND",
                             creation_date))
        
        self.sql_connection.commit()
        return True

    def session_exists(self, session_id):
        cursor = self.sql_connection.cursor()
        sql = "SELECT EXISTS(SELECT 1 FROM sds WHERE session_id = '" + session_id + "')"
        
        cursor.execute(sql)
        row = cursor.fetchone()
        
        session_exists = list(row.values())[0] == 1 
        if session_exists:
            logger.debug("Found " + session_id + " but the session already exists in database")
            return True
        return False
    
    def connect(self):
        try:
            with open('/home/ubuntu/alr/me_ingest/credentials.json') as cred_file: #TODO: remove hard coded path
                credentials = json.load(cred_file)
            session = boto3.Session(aws_access_key_id=credentials['vw']['id'], 
                                    aws_secret_access_key=credentials['vw']['key'])
            self.s3_client = session.client('s3', config=Config(signature_version='s3v4'))
            # --- get list of available sessions on aws
            response = self.s3_client.list_objects_v2(Bucket='alr-event-data', Delimiter = '/')
            self.sessions_on_s3 = [prefix['Prefix'][:-1] for prefix in response ['CommonPrefixes']]
            # --- Transcribe
            self.transcriber = sr.Recognizer() 
            logger.info("Succesfully connected to Gdrive database")
        except Exception as e:
            logger.error("Unsuccessful attempt of connection to Google with exeption:")
            logger.error(e)
            return False
        try:
            self.sql_connection = pymysql.connect(host='imea-database.cxsljb337cnj.eu-central-1.rds.amazonaws.com', 
                                                  user='admin', 
                                                  password='alr12345', 
                                                  database=self.database, 
                                                  cursorclass=pymysql.cursors.DictCursor)
            logger.info("Succesfully connected to MySQL database")
            return True
        except Exception as e:
            logger.error("Unsuccessful attempt of connection to Google with exeption:")
            logger.error(e)
            return False
