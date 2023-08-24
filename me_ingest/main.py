from event_data_ingest import *

ingestor = EventDataIngest(event_db = GoogleEventDb(),
                           temp_folder = '/audio_tags/',
                           aws_bucket_name = 'mobileye-msbz-inputs')
if ingestor.check_connections():
    ingestor.retrieve_sessions(timeframe=4)
    ingestor.upload_new_sessions()
    ingestor.print_report()

