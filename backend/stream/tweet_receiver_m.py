from daemon import Daemon
import os, sys
import json
import pymongo
from confluent_kafka import Consumer, KafkaError
import threading

curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from backend.common import common


class TweetReceiver(Daemon):

    TOPIC = 'tweet'
    SERVER = common.get_kafka_server()
    MONGO_CNTN_STR = common.get_mongo_connection_str()

    stream = None

    def __init__(self, pid_file):
        Daemon.__init__(self, pid_file)
        self.consumer = None

    def run(self):
        print("started consumer ...")
        self.setup_consumer()
        self.__delete_old_tweets()
        self.consume()

    @staticmethod
    def acknowledge(consumer, partitions):
        print("Assignment: ", partitions)

    def setup_consumer(self):

        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'confluent-client',
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            },
            'api.version.request': True
        })

    def __delete_old_tweets(self):
        threading.Timer(600.0, self.__delete_old_tweets).start()  # called every minute
        print(111)


    def consume(self):
        self.consumer.subscribe([TweetReceiver.TOPIC])

        while True:
            msg = self.consumer.poll(500.0)
            if not msg:
                continue

            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    pass
                    # Error
                    # raise KafkaException(msg.error())
            else:
                # print('On partiion nr. ' + str(msg.partition()))
                # print(msg.value())
                self.__insert_tweet(msg.value())

    def __insert_tweet(self, body):
        try:
            tweet = json.loads(body)
            client = pymongo.MongoClient(TweetReceiver.MONGO_CNTN_STR)
            db = client.get_database('twitter')

            db.tweets.insert(tweet)

            hashtags = tweet['hashtags']

            if len(hashtags) > 0:
                self.__insert_hashtags(tweet['id'], tweet['created_at'], hashtags, db)

            client.close()

        except Exception as e:
            print(str(e))
            if client:
                client.close()


    def __insert_hashtags(self, tweet_id, created_at, hashtags_b, db):
        hashtags = []
        try:
            for hashtag_b in hashtags_b:
                hashtags.append({
                    'tweet_id': tweet_id,
                    'created_at': created_at,
                    'hashtag': hashtag_b['text']
                })

            db.hashtags.insert_many(hashtags)
        except Exception as e:
            print(str(e))


if __name__ == "__main__":

    script_dir = os.path.dirname(os.path.realpath(__file__))
    pid_file = script_dir + "/tweets-receiver.pid"

    daemon = TweetReceiver(pid_file)

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
