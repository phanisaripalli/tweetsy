import tweepy
import time
from confluent_kafka import Producer
from time import sleep
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

import os
import sys
curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from backend.common import common


class TweetProducer(tweepy.StreamListener):

    TOPIC = 'tweet'
    SERVER = common.get_kafka_server()

    CONF = {
        "debug": "topic,msg,broker"
    }

    def set_stream(self, stream, search_key):
        self.stream = stream
        self.search_key = search_key
        self.__create_producer()


    def __get_timestamp(self, twitter_created_at):
        return time.strftime('%Y-%m-%d %H:%M:%S',
                             time.strptime(twitter_created_at,'%a %b %d %H:%M:%S +0000 %Y')
                             )

    def __create_producer(self):
        try:
            self.producer = Producer({'bootstrap.servers': 'localhost:9092'})
        except Exception as e:
            print(str(e))

    def on_status(self, status):

        try:
            tweet = status._json

            message = {
                'id': tweet['id'],
                'created_at': tweet['created_at'],
                'tweet_timestamp': self.__get_timestamp(tweet['created_at']),
                'text': tweet['text'],
                'user': tweet['user']['screen_name'],
                'user_id': tweet['user']['id'],
                'retweeted': tweet['retweeted'],
                'coordinates': tweet['coordinates'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'hashtags': tweet['entities']['hashtags'],
                'user_mentions': tweet['entities']['user_mentions'],
                'received_at': int(time.time()),
                'search_key': self.stream.search_key
            }

            self.produce(json.dumps(message), tweet['user']['id'])
        except Exception as e:
            # Ideally log
            print("Error " + str(e))

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            pass
            # print('Message delivered to {} [{}] {}'.format(msg.topic(), msg.partition(), msg.value))



    def produce(self, message, key):
            self.producer.produce(TweetProducer.TOPIC,
                         value=message,
                         key=str(key),
                         partition=0,
                         callback=self.delivery_report
                         )

            self.producer.poll(0.5)

