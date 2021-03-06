from daemon import Daemon
import os, sys
import json
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError
import threading

curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from backend.common import common


class TweetReceiver(Daemon):

    TOPIC = 'tweet'
    SERVER = common.get_kafka_server()
    PG_CNCTN_STR = common.get_postgres_connection_str()
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
        threading.Timer(900.0, self.__delete_old_tweets).start()
        #cur = self.pg_connection.cursor()
        sql = """DELETE FROM twitter.tweets 
                WHERE (tweet->>'tweet_timestamp')::timestamp <= current_timestamp - (15 * interval '1 minute')
            """
        conn = psycopg2.connect(TweetReceiver.PG_CNCTN_STR)
        cur = conn.cursor()
        cur.execute(sql)

        sql = """DELETE FROM twitter.hashtags 
                        WHERE tweet_timestamp <= current_timestamp - (15 * interval '1 minute')
                    """
        cur.execute(sql)
        conn.commit()
        cur.close()


    def consume(self):
        self.consumer.subscribe([TweetReceiver.TOPIC])

        while True:
            msg = self.consumer.poll(500.0)
            if not msg:
                continue

            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                    # End of partition event
                    # sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
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
            conn = psycopg2.connect(TweetReceiver.PG_CNCTN_STR)
            cur = conn.cursor()
            sql = "INSERT INTO twitter.tweets VALUES (%s)"
            data = json.dumps(tweet, ensure_ascii=False)
            cur.execute(sql, (data,))
            hashtags = tweet['hashtags']

            if len(hashtags) > 0:
                self.__insert_hashtags(tweet['id'], tweet['tweet_timestamp'], hashtags, cur)

            conn.commit()
            cur.close()
            conn.close()


        except Exception as e:
            print(str(e))
            print('err')


    def __insert_hashtags(self, tweet_id, tweet_timestamp, hashtags_b, cur):
        hashtags = []
        try:
            for hashtag_b in hashtags_b:
                hashtags.append((
                    tweet_id,
                    tweet_timestamp,
                    hashtag_b['text']
                ))


            sql = "INSERT INTO twitter.hashtags VALUES %s"

            execute_values(cur, sql, hashtags)


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
