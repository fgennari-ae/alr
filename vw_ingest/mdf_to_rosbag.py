import rosbag
from asammdf import MDF
import pandas as pd
import rospy
import json
 
from std_msgs.msg import Float64, String, Bool
from sensor_msgs.msg import NavSatFix

import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

month = 7
day = 27
hour = 15
minute = 00
second = 21

window_before = 60*5 #seconds 
window_after = 60*5 #seconds 

# Opening JSON file
with open('signal_list.json') as json_file:
    sl = json.load(json_file)

bag = rosbag.Bag('new_test.bag', 'w')

#mdf4= MDF("/home/fgennari/41295e8b-13da-4961-a6dc-20d44c10ddd4/VN41S130237/2023-07-24_12-58-37/clexport/Line_4/data1R.MDF")

#mdf= MDF("/home/fgennari/Downloads/VN41S130235/2023-05-16_15-38-26/clexport/Line_8/data2F.MF4")
mdf= MDF("/home/fgennari/Downloads/7cb0307f-a1a8-4f47-b274-d90e4d525e8d/VN41S130236/2022-07-28_08-44-13/clexport/Line_8/data2F.MDF")

db = mdf.channels_db

time_sig = sl["timestamps_line_8"][0]
print(time_sig)
time_channels = [(None, db[time_sig[key]][0][0], db[time_sig[key]][0][1]) for key in time_sig]
print(time_channels)
filtered_mdf = mdf.filter(time_channels)
df = filtered_mdf.to_dataframe()
print(df)
event_timestamp = df.loc[df[time_sig["month"]] == month].loc[
                            df[time_sig["day"]]== day].loc[
                                df[time_sig["hour"]] == hour].loc[
                                    df[time_sig["minute"]] == minute].loc[
                                            df[time_sig["second"]] == second].head(1).index.values[0]

print(event_timestamp)

start = event_timestamp - window_before
stop = event_timestamp + window_after


#check the challes exist in the db
for signal in sl['signals']:
    if db[signal['can_name']]:
        signal['Skip'] = False
    else:
        signal['Skip'] = True
        
filtered_mdf = mdf.filter([(None, db[x['can_name']][0][0], db[x['can_name']][0][1])  for x in sl['signals']])
#cutting mdf around the event 
event_mdf = filtered_mdf.cut(start=start, stop=stop)
res_event_mdf = event_mdf.resample(raster=0.05)
event_data = res_event_mdf.to_dataframe(time_from_zero=False, reduce_memory_usage=True)
print(event_data)

try:
    for ts, row in event_data.iterrows():
        msg = NavSatFix()
        for signal in sl['signals']:
            if signal['skip']:
                continue
            if signal['type'] == "Float":
                bag.write(signal['name'], Float64(row[signal['can_name']]), t=rospy.Time(ts))
            if signal['type'] == "Bool":
                bag.write(signal['name'], Bool(row[signal['can_name']].decode() == 'True'), t=rospy.Time(ts))
            if signal['type'] == "String":
                bag.write(signal['name'], String(row[signal['can_name']].decode()), t=rospy.Time(ts))
            if signal['type'] == "GPS":
                if signal['name'] == "latitude":
                    msg.latitude = row[signal['can_name']]
                if signal['name'] == "longitude":
                    msg.longitude = row[signal['can_name']]
        bag.write('gps', msg, t=rospy.Time(ts))
        
finally:
    bag.close()
