from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import speech_recognition as sr
from event_db import EventDb
from datetime import datetime
from pytz import timezone

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
        self.team_drive_id = '0AJJkbE1iJ7IcUk9PVA'
        self.root_folder_id='1k1I4UoeHg-N9rZ1KL3bIqUnW1dCrrpMn'
        self.sessions_in_db = []
        self.cred_file = os.getcwd() + '/credentials/mycreds.txt'
        self.sql_connection = None

    def _create_session(self, session_id):
        if not self.drive:
            logger.warn("Drive API not set up") #TODO: Add logger
            return None
        try:
            file_metadata = {
                'title': session_id,
                'parents': [{'id': self.root_folder_id}], #parent folder
                'mimeType': 'application/vnd.google-apps.folder'
            }

            folder = self.drive.CreateFile(file_metadata)
            folder.Upload()
        except Exception as e:
            logger.error("There was an error creating the session folder on Drive:")
            logger.error(e)
            return None
        return folder

    def _create_event_in_session(self, session_key, event):
        if not self.drive:
            logger.warn("Drive API not set up") #TODO: Add logger
            return
        logger.debug("Creating file on Google drive for event " + event.audio_tag)
        gfile = self.drive.CreateFile({'title': event.audio_tag, 'parents': [{'id': session_key['id']}], 'supportsAllDrives': 'true'})
        # Read file and set it as the content of this instance.
        gfile.SetContentFile(event.local_path)
        logger.debug("Uploading file on Google drive for event " + event.audio_tag)
        gfile.Upload() # Upload the file.
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
        event.annotation=event.audio_tag.split("_")[-1].split(".")[0]
        logger.debug("Setting permissions for " + event.audio_tag)
        url = 'https://www.googleapis.com/drive/v3/files/' + gfile['id'] + '/permissions?supportsAllDrives=true'
        headers = {'Authorization': 'Bearer ' + self.access_token, 'Content-Type': 'application/json'}
        payload = {'type': 'anyone', 'value': 'anyone', 'role': 'reader'}
        
        res = requests.post(url, data=json.dumps(payload), headers=headers)
        event.link_to_audio_file=gfile['embedLink']
        creation_date = datetime.now(timezone('Europe/Berlin')).strftime('%Y-%m-%d %H:%M:%S')
        event.file_url = 'https://docs.google.com/uc?export=open&id=' + gfile['id']
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
        return gfile

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
            # --- Setting up credentials to upload files to drive
            gauth = GoogleAuth()
            # Try to load saved client credentials
            gauth.LoadCredentialsFile(self.cred_file)
            if gauth.credentials is None:
                # Authenticate if they're not there
                gauth.LocalWebserverAuth()
            elif gauth.access_token_expired:
                # Refresh them if expired
                gauth.Refresh()
            else:
                # Initialize the saved creds
                gauth.Authorize()
            # Save the current credentials to a file
            gauth.SaveCredentialsFile(self.cred_file)
            self.drive = GoogleDrive(gauth)
            # --- Saving Access token for editing the permissions:
            self.access_token = gauth.credentials.access_token # gauth is from drive = GoogleDrive(gauth) Please modify this for your actual script.
            # --- Setting up credentials to edit files on drive
            file_url = 'https://docs.google.com/spreadsheets/d/10ey-nvai6e6TdFfPzgo8teuR8b9TRR7rkFlBENjko74/edit#gid=0'
            scope = [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/drive.file'
                ]
            # --- get list of available sessions on drive
            #file_list = self.drive.ListFile(
            #        {'q': "'1k1I4UoeHg-N9rZ1KL3bIqUnW1dCrrpMn' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", 
            #         'corpora': 'teamDrive', 
            #         'teamDriveId': self.team_drive_id, 
            #         'includeTeamDriveItems': True, 
            #         'supportsTeamDrives': True}).GetList() #TODO: Move folder id out
            # --- Transcribe
            self.transcriber = sr.Recognizer() 
            logger.info("Succesfully connected to Gdrive database")
            self.sql_connection = pymysql.connect(host='localhost', user='alr', password='Alr12345!', database=self.database, cursorclass=pymysql.cursors.DictCursor)
            logger.info("Succesfully connected to MySQL database")
            return True
        except Exception as e:
            logger.error("Unsuccessful attempt of connection to Google with exeption:")
            logger.error(e)
            return False
