import os, sys
import urllib

curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')



class API():

    def __init__(self, twitter_api):
        self.twitter_api = twitter_api


    def get_tweets(self, params):

        if params['max_id'] is None:
            del params['max_id']

        api_q = urllib.parse.urlencode(params)
        results = self.twitter_api.GetSearch(raw_query=api_q)

        return results

    def get_trends(self):
        trends = self.twitter_api.GetTrendsCurrent()

        return trends