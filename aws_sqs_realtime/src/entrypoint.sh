#!/bin/bash

# Start PostgreSQL
#/etc/init.d/postgresql start
# Run the send_message.py script
nohup python /app/send_message.py & > send_message.log
#sleep for 30 secs to give the send_message script enough time to create a queue
sleep 15
# Run the read_messages.py script
python /app/read_messages.py > read_messages.log
# Keep container running
tail -f /dev/null
