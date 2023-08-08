from typing import List
import logging
import os

logger = logging.getLogger('DataTypes')

class Event:
    def __init__(self, 
                 session_id = 'ND',
                 audio_tag = 'ND',
                 vehicle_id = 'ND',
                 sw_release = 'ND',
                 country = 'ND',
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
                 mission = 'ND',
                 remote_path = 'ND',
                 local_path = 'ND'):

        self.vehicle_id = vehicle_id
        self.sw_release = sw_release
        self.session_id = session_id
        self.country= country
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
        self.link_to_audio_file = link_to_audio_file #TODO: remove if not used for jira
        self.mission = mission
        self.remote_path = remote_path
        self.local_path = local_path

    def get_values(self):
        all_values = list(self.__dict__.values())
        return all_values[:-2]

class Session: 
    def __init__(self, session_id, metadata, download_folder):
        self.raw_metadata = {}
        self.session_id = session_id
        session_id.split("_")
        self.vehicle_id = session_id.split("_")[1]
        date_raw = session_id.split("_")[2]
        self.date = date_raw[4:6] + "/" + date_raw[2:4] + "/" + '20' + date_raw[0:2]
        time_raw = session_id.split("_")[3]
        self.start_time = time_raw[0:2] + ":" + time_raw[2:4] + ":" + time_raw[4:6]
        self.raw_metadata = metadata
        try:
            self.country = metadata["drive_info"]["Data_Info"]["country"]
        except Exception as e:
            logger.warning("Unable to get session country from metadata for session: " + session_id)
            logger.debug(e)
            self.country = "ND"
        self.skip = False
        self.events = []
        self.local_folder = download_folder + session_id + "/" 

    def create_local_folder(self):
        if not os.path.exists(self.local_folder):
            # Create a new directory because it does not exist
            os.makedirs(self.local_folder)

    def append_event(self, event):
        event.session_id = self.session_id
        event.vehicle_id = self.vehicle_id
        event.date = self.date
        event.time = self.start_time
        try:
            event.driver = self.raw_metadata["drive_info"]["Base_Data"]["driver"] 
            event.codriver = self.raw_metadata["drive_info"]["Base_Data"]["co_pilot"] 
            event.country = self.raw_metadata["drive_info"]["Data_Info"]["country"] 
            event.mission = self.raw_metadata["drive_info"]["Data_Info"]["driver_comment"] 
        except Exception as e: 
            logger.warning("There was an error saving session metadata for the current event in session: " + self.session_id)
            logger.debug(e)
        self.events.append(event)

    def print_events(self):
        for e in self.events:
            print(e.get_values())

    def get_event(self, event_id):
        for e in self.events:
            if event_id == e.audio_tag:
                return e

