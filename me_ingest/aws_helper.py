import re
import json
import boto3
import datetime
from datatypes import Session, Event
from tqdm import tqdm
import logging

logger = logging.getLogger('AwsHelper')
 
class AwsHelper():

    def __init__(self, bucket_name, local_download_folder):
        self.local_download_folder = local_download_folder
        self.bucket = bucket_name
        self.client = None

    def connect(self):
        try:
            self.client = boto3.client('s3')
            logger.info("Succesfully connected to aws")
            return True
        except ClientError:
        # The bucket does not exist or you have no access.
            logger.error("Unable to connect to aws")
            return False
    
    def _get_session_metadata(self, filepath): 
        s3_response = self.client.get_object(Bucket=self.bucket, Key=filepath)
        # Get the Body object in the S3 get_object() response
        s3_object_body = s3_response.get('Body')
        # save the data in bytes format
        return json.loads(s3_object_body.read())

    def _get_event_timestamp_from_frame_id(self, event):
        logpath = event.remote_path.rsplit("/",2)[0]+"/AV_logs/DisplayLogs/"
        response = self.client.list_objects_v2(Bucket=self.bucket,Prefix=logpath) 
        possible_logs = []
        lines = []
        pattern_str = r'\d{2}:\d{2}:\d{2}'
        try:
            for object in response['Contents']:
                if object['Key'].endswith('.log'):
                    possible_logs.append(object['Key'])
                    s3_response = self.client.get_object(Bucket=self.bucket, Key=object['Key'])
                    s3_object = s3_response.get('Body').readlines()
                    for line in s3_object:
                        if b'frame: ' + event.audio_tag.split("_")[0].encode() in line:
                            maybe_time = line.decode().split(",")[1].split(" ")[2]
                            if re.match(pattern_str, maybe_time):
                                return maybe_time
        except Exception:
            return "Not found"
        return "Not found"


    def _get_session_annotations_paths(self, metadata):
        paths = []
        for x in metadata["FileList"]:
            if x['file'].endswith('.wav'):
                paths.append(x['file'])
        return paths 

    def _get_session_report(self, path):
        #check if session report is available (means that the session upload finished)
        response = self.client.list_objects_v2(Bucket=self.bucket,Prefix=path)
        for object in response['Contents']:
            if object['Key'].endswith('logged_Upload_Report.json'):
                return object['Key']
        return None


    def get_paths_to_available_sessions(self, timeframe):
        output=[]
        result = self.client.list_objects(Bucket=self.bucket, Prefix='', Delimiter='/')
        pattern_str = r'^\d{6}$'
        for vehicle_folder in result.get('CommonPrefixes'):
            subresult = self.client.list_objects(Bucket=self.bucket, 
                                                 Prefix=vehicle_folder.get('Prefix'), 
                                                 Delimiter='/')
            #checking if the folder is not empty
            if subresult.get('CommonPrefixes'):
                for date_folder in subresult.get('CommonPrefixes'):
                    maybe_date = date_folder.get('Prefix').split("/")[1] 
                    if re.match(pattern_str, maybe_date):
                        year = '20' + maybe_date[0:2]
                        month = maybe_date[2:4]
                        day = maybe_date[4:6]
                        date = datetime.datetime(int(year), int(month) , int(day))
                        if date >= datetime.datetime.today() - datetime.timedelta(days=timeframe):
                            subsubresult = self.client.list_objects(Bucket=self.bucket, 
                                                               Prefix=date_folder.get('Prefix'), 
                                                               Delimiter='/')
                            for time_folder in subsubresult.get('CommonPrefixes'):
                                if re.match(pattern_str, time_folder.get('Prefix').split("/")[2]):
                                    finalresult = self.client.list_objects(Bucket=self.bucket, 
                                                                      Prefix=time_folder.get('Prefix'), 
                                                                      Delimiter='/')
                                    for session_folder in finalresult.get('CommonPrefixes'):
                                        output.append(session_folder.get('Prefix'))
        return output
   

    def create_session_from_path(self, path):
        #read session metadata 
        session_report_path = self._get_session_report(path)
        if session_report_path:
            session_id = path.split("/")[-2]
            try:
                session_metadata = self._get_session_metadata(session_report_path)
            except Exception as e:
                logger.warn("Unable to process metadata in path " +
                            session_report_path + ":")
                logger.debug("Exception: " + str(e))
                session = Session(session_id = session_id,
                                  metadata = None,
                                  download_folder = None)
                session.skip = True
                session.skip_reason = "Missing metadata"
                return session
            #adding voice annotations
            audio_tags_relative_paths = self._get_session_annotations_paths(session_metadata)
            session = Session(session_id = session_id, 
                              metadata = session_metadata,
                              download_folder = self.local_download_folder)
            session.create_local_folder()
            for tag_rel_path in audio_tags_relative_paths: 
                audio_tag_name = tag_rel_path.split("/")[-1]
                current_event = Event(audio_tag=audio_tag_name,
                                      remote_path=path+tag_rel_path)
                session.append_event(current_event)
            return session
        return None
    
    def get_audio_tags_from_session_locally(self, session):
        iterator = tqdm(session.events, 
                        desc="Copying locally audio tags from session " + session.session_id, 
                        position=0, 
                        leave=True)
        for event in iterator:
            local_path_of_tag = session.local_folder + "/" + event.audio_tag
            logger.debug("Trying to download " + event.remote_path + " to " + local_path_of_tag)
            try:
                self.download_file(event.remote_path, local_path_of_tag)
                event.local_path = local_path_of_tag
                logger.debug("Succesfully copied file locally")
            except Exception as e:
                session.skip = True
                session.skip_reason = "Unable to get event"
                iterator.close()
                logger.warn("Unable to process event " +
                            event.audio_tag +
                            " in session " + session.session_id + ". Skipping whole session")
                logger.debug("Exception: " + str(e))
                return

    def download_file(self, remote_path, local_path): 
        self.client.download_file(self.bucket, remote_path, local_path)
        return
