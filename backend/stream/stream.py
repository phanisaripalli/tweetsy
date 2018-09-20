from daemon import Daemon
import os, sys
import tweepy
from tweet_producer import TweetProducer

curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from backend.common import common

class TwitterStream(Daemon):

    stream = None

    def __init__(self, pid_file):
        Daemon.__init__(self, pid_file)
        self.search_key = "sport"

    def run(self):
        print("run ...")
        self.streamer()
        import time
        #time.sleep(100)

    def streamer(self):
        consumer_key, consumer_secret, access_key, access_secret, stream_keyword  = common.get_twiiter_credentials()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)

        tweet_producer = TweetProducer()
        tweet_producer.set_stream(self, stream_keyword)
        self.stream_listener = tweepy.Stream(auth=auth, listener=tweet_producer)
        self.stream_listener.filter(track=[stream_keyword], languages=['en'])
        

if __name__ == "__main__":

    script_dir = os.path.dirname(os.path.realpath(__file__))
    pid_file = script_dir + "/tweets-sender.pid"

    daemon = TwitterStream(pid_file)
    
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
