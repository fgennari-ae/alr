from event_data_ingest import *

ingestor = EventDataIngest(event_db = MySQLEventDb(database="events"),
                           temp_folder = '/home/ubuntu/alr/me_ingest/audio_tags/',
                           aws_bucket_name = 'mobileye-msbz-inputs')
if ingestor.check_connections():
    ingestor.retrieve_sessions(timeframe=7)
    ingestor.upload_new_sessions()
    ingestor.print_report()

