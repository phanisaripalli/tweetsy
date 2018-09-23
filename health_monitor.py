import os
import subprocess

kafka_path = '/Users/phani/Projects/confluent-5.0.0/'

stream_path = '/Users/phani/Projects/tweetsy/backend/stream'

cmd_1 = 'ps aux | grep zookeeper'
cmd_2 = 'ps aux | grep kafka/server'

cmd_1_r = os.popen(cmd_1 ).read().strip().split( '\n' )
print(cmd_1_r)
if len(cmd_1_r) < 3:
    cmd = "{}./bin/zookeeper-server-start {}./etc/kafka/zookeeper.properties".format(kafka_path, kafka_path)
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    #output, error = process.communicate()
    print(1)

cmd_2_r = os.popen(cmd_2 ).read().strip().split( '\n' )
print(cmd_2_r)
if len(cmd_2_r) < 3:
    cmd = "{}./bin/kafka-server-start {}./etc/kafka/server.properties".format(kafka_path, kafka_path)
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    #output, error = process.communicate()
    print(2)
