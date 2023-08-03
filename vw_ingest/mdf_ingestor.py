from asammdf import MDF
from std_msgs.msg import Float64, String, Bool
from sensor_msgs.msg import NavSatFix
from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta
from tabulate import tabulate
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import numpy as np
import json
import rospy
import rosbag
import boto3
import os, shutil
import logging
import sys

# Setting Logger

log_filename = datetime.now().strftime('logs/MdfIngestor_%H_%M_%d_%m_%Y.log')
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
#console_handler.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                    handlers=[file_handler, console_handler])
logger = logging.getLogger('MdfIngestor')

class Event:
    def __init__(self, mdf, timestamp, id):
        self.id = id
        self.timestamp = timestamp
        self.mdf = mdf
        self.rosbag_path = None


class MdfIngestor:

    def __init__(self, working_folder, line):
        self.mdf = None
        self.events = []
        with open('signal_list.json') as json_file:
            self.signal_list = json.load(json_file)
        logger.info("Setting working folder" + working_folder)
        self.working_folder = working_folder
        self.start_time = None
        self.end_time = None
        self.session_duration = None
        self.line = line
        self.window_size = 40
        self.resample_rate = 0.05

    def _validate_start_time(self, start_time):
        if not self.start_time:
            self.start_time = start_time
            return
        if self.start_time == start_time:
            return
        Exception("MDF start time is not validated")
    
    def _validate_session_duration(self, duration):
        if not self.session_duration:
            self.session_duration = duration
            return
        if self.session_duration == duration:
            return
        Exception("MDF start time is not validated")
   
    def _get_mdf(self, mdf_path):
        try:
            mdf = MDF(mdf_path)
            self._validate_start_time(mdf.start_time)
            db = mdf.channels_db
            signals_to_get = []
            logger.debug("Extracting signal checking if has not been extracted before")
            for signal in self.signal_list['signals']:
                if signal['extracted']:
                    continue
                if signal['can_name'] in db.keys():
                    possible_signals = db[signal['can_name']]
                    for ps in possible_signals:
                        candidate_signal = mdf.get(name=signal['can_name'], group=ps[0], index=ps[1])
                        if candidate_signal.timestamps.any():
                            signals_to_get.append((None, ps[0], ps[1]))
                            logger.debug("Signal " + signal['can_name'] + " found with values in mdf")
                            self._validate_session_duration(candidate_signal.timestamps[-1])
                            signal['extracted'] = True
                            break
                else:
                    logger.debug("Signal " + signal['can_name'] + " NOT found in mdf")
            return mdf.filter(signals_to_get)  

        except Exception as e:
            logger.error("Something went wrong while filtering mdf")
            logger.error(e)
            
    def _extract_data(self):
        logger.info("Getting mdf files")
        logger.debug("Checking if filename is mdf or mf4")
        linepath = self.working_folder + "clexport/" + self.line
        logger.debug("Checking if Line folder exists")
        if not Path(linepath).is_dir():
            logger.warning("Folder for " + self.line + " doesn't exist")
            return False
        mdfs_to_stack = []
        for f in listdir(linepath):
            if isfile(join(linepath, f)):
                if f.endswith(".MDF") or f.endswith(".MF4"):
                    if f.startswith("data2F") or f.startswith("data1R"):
                        mdfs_to_stack.append(self._get_mdf(linepath+"/"+f))
        logger.info("Stacking mdfs")
        filtered_mdf = MDF.stack(mdfs_to_stack)
        self.mdf = filtered_mdf.resample(self.resample_rate)
        logger.info("All available MDF or MF4 files have been acquired")
        return True

    def _timestamp_is_in_session(self, ts):
        logger.debug("Start time of the mdf: " + str(self.start_time))
        logger.debug("End time of the mdf: " + str(self.end_time))
        return self.start_time <= ts <= self.end_time

    def _get_timestamp_in_seconds_from_start(self, ts):
        if self._timestamp_is_in_session(ts):
            logger.debug("Event with timestamp: " + str(ts) + " found in session")
            delay = ts - self.start_time
            return delay.seconds
        logger.warning("Unable to find timestamp in current session!")
        return None

    def _extract_dr_int_seconds(self, acc, brake, steer, eng):
        res = []
        logger.info("Looking for events ...")
        for ts, a, b, s, e  in zip(acc.timestamps, acc.samples, brake.samples, steer.samples, eng.samples):
            if a.decode()=='True' or b.decode()=='True' or s.decode()=='True':
                #check if 1s before the system was engaged:
                prev_eng_index = np.where(eng.timestamps == ts)[0][0] - int(self.resample_rate*10)
                if eng.samples[prev_eng_index].decode() == 'AVEngaged':
                    res.append(ts)
        logger.info("Found " + str(len(res)) + " TO events")
        return res

    def _get_events_by_driver_interventions(self):
        sl = self.signal_list['signals']
        logger.debug("Getting Combined list of driver interventions")
        seconds = self._extract_dr_int_seconds(self.mdf.get('DriverIntervention_Accelerator'),
                                               self.mdf.get('DriverIntervention_Brake'),
                                               self.mdf.get('DriverIntervention_Steering'),
                                               self.mdf.get('Engagement_Prim_Stat'))
        logger.debug("Found " + str(len(seconds)) + " events")

        for second in seconds:
            self._get_event_by_second(second)
       
    def _get_event_by_second(self, ts_s):
        cut_start = ts_s - self.window_size/2
        cut_stop = ts_s + self.window_size/2
        logger.debug("Cutting mdf for event at time: " + str(ts_s) + " between " + str(cut_start) + " and " + str(cut_stop))
        cutted_mdf= self.mdf.cut(start=cut_start, stop=cut_stop)
        id_name = str(self.start_time).replace(" ","-").replace(":","-")+"_"+str(int(ts_s))
        self.events.append(Event(mdf=cutted_mdf, 
                                 timestamp=ts_s, 
                                 id=id_name))
        return
        
    def _get_events_by_timestamps(self, timestamps):
        logger.debug("Processing the mdfs to extract events")
        #update end time
        self.end_time = self.start_time + timedelta(0,self.session_duration)
        for ts in timestamps:
            ts_s = self._get_timestamp_in_seconds_from_start(ts)
            if ts_s:
                self._get_event_by_second(ts_s)
            else:
                logger.error("Unable to extract the event at time " + str(ts) + ". Skipping...")

    def _write_single_signal_to_rosbag(self, signal, bag, name, type):
        logger.debug("creating topic for signal " + signal.name + " with name " + name + ", values: " + str(len(signal.timestamps)) )
        for ts, val in zip(signal.timestamps, signal.samples):
            if type == "Float":
                bag.write(name, Float64(val), t=rospy.Time(ts))
            if type == "Bool":
                bag.write(name, Bool(val.decode() == 'True'), t=rospy.Time(ts))
            if type == "String":
                bag.write(name, String(val.decode()), t=rospy.Time(ts))

    def _write_gps_signals_to_rosbag(self, latitude, longitude, bag):
        for ts, lat, lon in zip(latitude.timestamps, latitude.samples, longitude.samples):
            msg = NavSatFix()
            msg.latitude = lat
            msg.longitude = lon
            bag.write('gps', msg, t=rospy.Time(ts))

    def _save_rosbag_slices(self): 
        for event in tqdm(self.events,
                          desc="Creating Rosbag slice for events", 
                          position=0, 
                          leave=True):
            file_name = "bags/" + event.id + '.bag'
            event.rosbag_path = file_name
            bag = rosbag.Bag(file_name, 'w')
            for signal in self.signal_list['signals']:
                if signal["can_name"] not in self.mdf.channels_db:
                    signal["skip"] = True
            try:
                logger.debug("Creating rosbag for event " + event.id)
                latitude = None
                longitude = None
                for signal in self.signal_list['signals']:
                    if signal['skip']:
                        continue
                    if signal['type'] == "GPS":
                        if signal['name'] == 'latitude': 
                            latitude = signal
                        if signal['name'] == 'longitude': 
                            longitude = signal
                    self._write_single_signal_to_rosbag(signal=event.mdf.get(signal['can_name']), 
                                                        bag=bag, 
                                                        name=signal['name'], 
                                                        type=signal['type'])
                    if latitude and longitude:
                        logger.debug("Writing gps data to rosbag")
                        self._write_gps_signals_to_rosbag(latitude=event.mdf.get(latitude['can_name']),
                                                          longitude=event.mdf.get(longitude['can_name']),
                                                          bag=bag)
            finally:
                bag.close()
        
    def _print_report(self):
        detail_table_data = [[e.id,
                              e.timestamp,                  
                              e.rosbag_path] for e in self.events]
        detail_table = tabulate(detail_table_data, 
                                headers=['Event Id', 'Timestamp', 'Rosbag'], 
                                tablefmt='orgtbl')
        print(detail_table)
        print(" ")
        logger.debug("\n" + detail_table)

    def process(self, timestamps=None, trigger=None):

        #look for mdf files in line4 and line 8
        if self._extract_data():
        
            if timestamps:
                #find events timestamps (based on trigger or timestamp)
                self._get_events_by_timestamps(timestamps)
            elif trigger=='DriverIntervention':
                self._get_events_by_driver_interventions()
            else:
                logger.warning("Event extraction method incorrect or not specified")
            
            #for every event get the log slice and save to a rosbag
            self._save_rosbag_slices() 
            self._print_report() 
            #update EventDb (with link to foxglove)

            #upload rosbags



