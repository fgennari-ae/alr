#!/bin/bash

# Start the run once job.
echo "Docker container has been started"

# Setup a cron schedule
PATH=/opt/ros/noetic/bin:/home/fgennari/python/bin:/home/fgennari/python/lib/python3.8/site-packages:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/fgennari/.aws/bin/:/usr/lib/jvm/java-8-openjdk-amd64/bin

*/20 * * * 1-6 python3 main.py >> /var/log/cron.log 2>&1
# This extra line makes it a valid cron" > scheduler.txt

crontab scheduler.txt
cron -f
