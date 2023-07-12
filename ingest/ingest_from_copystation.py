from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pathlib import Path
from typing import List
import os, shutil
import paramiko
import subprocess
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import speech_recognition as sr
from os import path
from pydub import AudioSegment

class Event:
    def __init__(self, 
                 session_id = 'ND',
                 audio_tag = 'ND',
                 vehicle_id = 'ND',
                 sw_release = 'ND',
                 city = 'ND',
                 map_version = 'ND',
                 driver = 'ND',
                 codriver = 'ND',
                 time = 'ND',
                 date = 'ND',
                 annotation = 'ND',
                 takeover = 'ND',
                 comment = 'ND',
                 link_to_audio_file = 'ND',
                 mission = 'ND'):

        self.vehicle_id = vehicle_id
        self.sw_release = sw_release
        self.session_id = session_id
        self.city= city
        self.map_version = map_version
        self.driver = driver
        self.codriver = codriver
        self.time = time
        self.date = date
        self.annotation = annotation
        self.takeover = takeover
        self.comment = comment
        self.audio_tag = audio_tag
        self.link_to_audio_file = link_to_audio_file
        self.mission = mission

    def get_values(self):
        return list(self.__dict__.values())

class Session: 
    local_path = ''
    def __init__(self, session_id):
        self.session_id = session_id
        self.events = []
        session_id.split("_")
        self.vehicle_id = session_id.split("_")[1]
        date_raw = session_id.split("_")[2]
        self.date = date_raw[4:6] + "/" + date_raw[2:4] + "/" + '20' + date_raw[0:2]
        time_raw = session_id.split("_")[3]
        self.start_time = time_raw[0:2] + ":" + time_raw[2:4] + ":" + time_raw[4:6]
        self.local_path

    def append_event(self, event):
        event.session_id = self.session_id
        event.vehicle_id = self.vehicle_id
        event.date = self.date
        event.time = self.start_time
        self.events.append(event)

    def print_events(self):
        for e in self.events:
            print(e.get_values())

    def get_event(self, event_id):
        for e in self.events:
            if event_id == e.audio_tag:
                return e


class AudioTagsRetriever:
# class to manipulate audio tags from copystations
    drive = None 
    team_drive_id = '0AJJkbE1iJ7IcUk9PVA'
    copystation_ips = ['172.26.8.182', '172.26.8.183']
    sheet_client = None
    sessions_on_drive = None
    new_sessions = dict()

    def __init__(self):
        self.cred_file = os.getcwd() + '/credentials/mycreds.txt'
        self.cred_sheet = os.getcwd() + '/credentials/cred_sheet.json'
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

        # --- Setting up credentials to edit files on drive
        file_url = 'https://docs.google.com/spreadsheets/d/10ey-nvai6e6TdFfPzgo8teuR8b9TRR7rkFlBENjko74/edit#gid=0'
        scope = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file'
            ]
        file_name = self.cred_sheet
        creds = ServiceAccountCredentials.from_json_keyfile_name(file_name,scope)
        sheet_client = gspread.authorize(creds)
        self.event_db = sheet_client.open_by_url(file_url).sheet1
        # --- get list of available sessions on drive
        file_list = self.drive.ListFile(
                {'q': "'1k1I4UoeHg-N9rZ1KL3bIqUnW1dCrrpMn' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", 
                 'corpora': 'teamDrive', 
                 'teamDriveId': self.team_drive_id, 
                 'includeTeamDriveItems': True, 
                 'supportsTeamDrives': True}).GetList() #TODO: Move folder id out
        self.sessions_on_drive = [i['title'] for i in file_list]
        # --- Transcribe
        self.transcriber = sr.Recognizer() 
        



    def _upload_file_to_drive(self, drive_folder, event, local_folder):
        if not self.drive:
            print("Drive API not set up") #TODO: Add logger
            return
        gfile = self.drive.CreateFile({'title': event.audio_tag, 'parents': [{'id': drive_folder['id']}], 'supportsAllDrives': 'true'})
        # Read file and set it as the content of this instance.
        gfile.SetContentFile(local_folder + event.audio_tag)
        gfile.Upload() # Upload the file.
        print("Successfully uploaded " + event.audio_tag + " to drive folder " + drive_folder['title']  ) #TODO: Add logger
        comment_from_audio = 'ND'
        with sr.AudioFile(local_folder + event.audio_tag) as source:
            audio = self.transcriber.record(source)  # read the entire audio file                  
            try:
                event.comment = str(self.transcriber.recognize_google(audio, language = 'en-IN'))
            except:
                print("An exception occurred while transcribing the audio, ignoring")
            #add event info 
        event.annotation=event.audio_tag.split("_")[-1].split(".")[0]
        event.link_to_audio_file=gfile['embedLink']
        self._add_new_entry_in_db(event)
   
    def _create_drive_folder(self, folder_name, parent_folder_id):
        if not self.drive:
            print("Drive API not set up") #TODO: Add logger
            return
        file_metadata = {
            'title': folder_name,
            'parents': [{'id': parent_folder_id}], #parent folder
            'mimeType': 'application/vnd.google-apps.folder'
        }

        folder = self.drive.CreateFile(file_metadata)
        folder.Upload()
        return folder

    def _upload_new_sessions_to_drive(self, parent_folder_id, local_folder):
        for session_name in self.new_sessions:
            session_drive_folder = self._create_drive_folder(session_name, parent_folder_id) 
            session_path = self.new_sessions[session_name].local_path
            for event in self.new_sessions[session_name].events: 
                self._upload_file_to_drive(session_drive_folder, event, session_path)

    def _establish_ssh_connection(self, copystation_ip):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(copystation_ip, username='copystation', password='jobim') #TODO: Move credentials
        return ssh

    def _get_new_sessions_from_copystation(self, copystation_ip):
        copystation = self._establish_ssh_connection(copystation_ip=copystation_ip)
        ssh_stdin, ssh_stdout, ssh_stderr = copystation.exec_command("find /media/copystation -name '*.wav'")
        sessions_from_copystation = [Path(x) for x in ssh_stdout.read().decode().splitlines()]
        
        for x in sessions_from_copystation:
            session_id = x.parts[-3]
            audio_tag = x.parts[-1]
            if session_id not in self.sessions_on_drive:
                if session_id not in self.new_sessions.keys():
                    self.new_sessions[session_id] = Session(session_id)
                    self.new_sessions[session_id].local_path = "/audio_tags/" + session_id + "/"
                    print("A new Session has been found")
                    if not os.path.exists(self.new_sessions[session_id].local_path):
                        # Create a new directory because it does not exist
                        os.makedirs(self.new_sessions[session_id].local_path)
                print("Found new audo tag (" + audio_tag + ") from session " + session_id + ". Copying it locally")
                command_list = ["rsync", 
                                "-avm", 
                                "copystation@" + copystation_ip + ":" + str(x), 
                                self.new_sessions[session_id].local_path]
                subprocess.call(command_list) 
                self.new_sessions[session_id].append_event(Event(audio_tag=audio_tag))

        return

               
    def _clear_working_folder(self):
        folder = '/audio_tags/'
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    def _event_exists_in_db(self, event: Event):
        entries = self.event_db.findall(event.session_id)
        session_match = [self.event_db.row_values(e.row) for e in entries]
        result = event.audio_tag in (item for sublist in session_match for item in sublist)
        return result

    def _add_new_entry_in_db(self, event: Event):
        self.event_db.append_row(event.get_values())


    def execute(self):
        self._clear_working_folder()
        #Get new sessions with events in self.new_sessions
        for ip in self.copystation_ips: 
            self._get_new_sessions_from_copystation(ip)
        self._upload_new_sessions_to_drive(parent_folder_id='1k1I4UoeHg-N9rZ1KL3bIqUnW1dCrrpMn',
                local_folder='/audio_tags/')
        print("Succesfully checked copystations for new sessions")

    def test(self):
        event1 = Event(session_id='123', audio_tag="420934")
        event2 = Event(session_id='123', audio_tag="420734")
        session = Session(session_id='123', events=[event1,event2])
        print(session.get_event('420934').get_values())

data_retriever = AudioTagsRetriever()
data_retriever.execute()
