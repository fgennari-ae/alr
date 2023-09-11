# Event Ingest Job
The ingest python project provides an interface with the ingest pipeline allowing to: 
- Retrieve sessions from the provided AWS bucket
- Check sessions against existing ones in the provided database
- Retrive metadata (and eventually raw data) and save it locally
- Upload data to the provided database

## Aws Helper
Provides an interface to connect to the ME AWS s3 bucket and retrieve data and metadata about voice annotations. 

```
class AwsHelper():

    def connect(self):
    # connects to aws bucket and check access
    
    def _get_session_metadata(self, filepath):
    # collects the session metadata from file <..>_logged_Upload_Report.json 

    def _get_event_timestamp_from_frame_id(self, event):
    # retrieves the timestamp of the event from the available logs in /AV_logs/DisplayLogs/
    # the method is deprecated as not all frame ids are logged and therefore a timestamps cannot always be found

    def _get_session_annotations_paths(self, metadata):
    # retrieves the path (key) of audio tags from the metadata file

    def _get_session_report(self, path):
    # collects the session metadata file <..>_logged_Upload_Report.json 

    def get_paths_to_available_sessions(self, timeframe):
    # finds the paths (keys) of available sessions in the given timeframe 

    def create_session_from_path(self, path):
    # returns a Session object with all available (even if none) events
    
    def get_audio_tags_from_session_locally(self, session):
    # downloads the audio memos from given session

    def download_file(self, remote_path, local_path): 
    # helper method to get file from s3 copied in a local path

```

## Event Database
Event Database (`EventDb`) is an abstract class to be used for the implementations of the destination database for the Sessions and the Events. 

Any other Event Database inheriting from `EventDb` should implement the following methods: 

```
    @abstractmethod
    def _create_session(self, session_id):
        pass

    @abstractmethod
    def _create_event_in_session(self, session_id, event):
        pass

    @abstractmethod
    def session_exists(self, session_id):
        pass
    
    @abstractmethod
    def connect(self):
        pass
```

## Build and run Event Ingest in Docker container

The Event Ingest Job has been dockerized in order to easily port it to a cloud solution.

In order to build the image locally run: 

```
docker build -t event-ingest:latest .
```

Running the docker container requires passing the aws credentials required to pull events from s3, therefore the command to execute the job is: 

```
docker run -it --network="host" -e AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id) \
				-e AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key) \ 
				event-ingest:latest
```
The flag `--network="host"` will allow to use the local MySQL database instance. Beware that the container is ment to be reworked as soon as the porting to cloud happens.

## To Do

- Implement tests
- Move from Session based to event based
- Get more abstraction
- Implement ES logging 
- Improved management of credentials with docker
- Implement recurrent job in docker

