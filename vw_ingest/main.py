from datetime import datetime, timedelta
from mdf_ingestor import *
import argparse

argParser = argparse.ArgumentParser()
argParser.add_argument("-f", "--folder", help="Working folder where clexport folder is located")

args = argParser.parse_args()

ingestor = MdfIngestor(working_folder=args.folder, line="Line_8")

#ingestor.process(timestamps=[datetime(2023,5,15,13,38,26)])
ingestor.process(trigger="DriverIntervention")
