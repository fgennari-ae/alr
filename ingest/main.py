from event_data_ingest import *

ingestor = EventDataIngest(event_db = GoogleEventDb(),
                           temp_folder = '/audio_tags/',
                           aws_bucket_name = 'mobileye-msbz-inputs')
if ingestor.check_connections():
    ingestor.retrieve_sessions(timeframe=6)
    ingestor.upload_new_sessions()

