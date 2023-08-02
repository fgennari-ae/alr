from asammdf import MDF
from std_msgs.msg import Float64, String, Bool
from sensor_msgs.msg import NavSatFix
from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta
from tabulate import tabulate
from pathlib import Path
import pandas as pd
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
#console_handler.setLevel(logging.INFO)
console_handler.setLevel(logging.DEBUG)
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
        self.mdf = MDF.stack(mdfs_to_stack)
        logger.info("All available MDF or MF4 files have been acquired")
        return True

    def _get_events_from_timestamps(self, timestamps):
        logger.debug("Processing the mdfs to extract events")
        for ts in timestamps:
            try:
                logger.debug("Start time of the mdf: " + str(self.start_time))
                start = ts - self.window_size/2
                stop = ts + self.window_size/2
                logger.debug("Event timestamp: " + str(ts))
                logger.debug("Cutting mdf for event at time: " + str(ts))
                cutted_mdf= self.mdf.cut(start=start, stop=stop)
                self.events.append(Event(mdf=cutted_mdf, timestamp=ts, id=str(ts).replace(" ", "_")))
                logger.debug("event for time: " + str(ts) + " found")
            except Exception as e:
                logger.error("There was an error extracting the event at time " + str(ts))
                logger.error(e)
                continue

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
        for event in self.events:
            res_mdf = event.mdf.resample(self.resample_rate)
            file_name = event.id + '.bag'
            bag = rosbag.Bag(file_name, 'w')
            for signal in self.signal_list['signals']:
                if signal["can_name"] not in res_mdf.channels_db:
                    signal["skip"] = True
            try:
                logger.info("Creating rosbag for event " + event.id)
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
                        #skipping gps data translation
                        continue
                    self._write_single_signal_to_rosbag(signal=res_mdf.get(signal['can_name']), 
                                                        bag=bag, 
                                                        name=signal['name'], 
                                                        type=signal['type'])
                    if latitude and longitude:
                        self._write_gps_signals_to_rosbag(latitude=res_mdf.get(latitude['can_name']),
                                                          longitude=res_mdf.get(longitude['can_name']),
                                                          bag=bag)
            finally:
                bag.close()

    def process(self, timestamps):

        #look for mdf files in line4 and line 8
        if self._extract_data():
        
            #find events timestamps (based on trigger or timestamp)
            #for every event find the right file to extract is
            self._get_events_from_timestamps(timestamps)
            #events = self._get_events_from_trigger(mdfs)
            #for every event get the log slice and save to a rosbag
            self._save_rosbag_slices() 
            #update EventDb (with link to foxglove)

            #upload rosbags



