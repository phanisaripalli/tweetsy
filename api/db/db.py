import pymongo
import os, sys
import urllib

curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from api.common import common

# db = client.get_database('twitter')


class DB():

    def __init__(self, twitter_api, client):
        self.twitter_api = twitter_api
        self.client = client

    def __get_client(self):
        mongo_cntn_str = common.get_mongo_connection_str()
        client = pymongo.MongoClient(mongo_cntn_str)

        return client

    '''
    def __getAPIObject(self):
        consumer_key, consumer_secret, access_token_key, access_token_secret = common.get_twiiter_credentials()
        api = twitter.Api(consumer_key=consumer_key,
                          consumer_secret=consumer_secret,
                          access_token_key=access_token_key,
                          access_token_secret=access_token_secret
                         )

        return api
    '''

    def get_tweets(self, params):

        if params['max_id'] is None:
            del params['max_id']

        api_q = urllib.parse.urlencode(params)
        results = self.twitter_api.GetSearch(raw_query=api_q)

        return results

    def get_trends(self):
        trends = self.twitter_api.GetTrendsCurrent()

        return trends

    def get_overview(self):
        pass

    def poular_hashtags(self):
        pass

    def minutly_distribution(self):
        pass

    def avg_distribution(self):
        pass