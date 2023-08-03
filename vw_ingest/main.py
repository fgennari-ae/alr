from datetime import datetime, timedelta
from mdf_ingestor import *

ingestor = MdfIngestor(working_folder='/home/fgennari/Downloads/VN41S130235/2023-05-16_15-38-26/', line="Line_8")
#ingestor = MdfIngestor(working_folder='/home/fgennari/Downloads/4bd75637-45a7-4da2-8040-d5269bd1be70/VN41S130236/2023-06-27_16-17-32/', line="Line_8")
#ingestor = MdfIngestor(working_folder='/home/fgennari/Downloads/7cb0307f-a1a8-4f47-b274-d90e4d525e8d/VN41S130236/2022-07-28_08-44-13/', line="Line_8")

#ingestor.process(timestamps=[datetime(2023,5,15,13,38,26)])
ingestor.process(trigger="DriverIntervention")
