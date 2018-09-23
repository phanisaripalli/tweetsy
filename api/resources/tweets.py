from flask_restful import Resource
from flask_restful import request
from api.t_api.api import API

class Tweets(Resource):
    def __init__(self, **kwargs):
        self.twitter_api = kwargs['twitter_api']

    def get(self):

        api = API(self.twitter_api)

        params = {
            'q': request.args.get('keyword'),
            'count': 15,
            'result_type': request.args.get('result_type'),
            'max_id': request.args.get('max_id')
        }

        tweets = api.get_tweets(params)

        json_results = [tweet.AsDict() for tweet in tweets]
        max_id = None

        nr_tweets = len(json_results)
        if nr_tweets > 0:
            max_id = json_results[nr_tweets - 1]['id']

        result = {
            'max_id': max_id,
            'tweets': json_results,
            'result': 200
        }

        return result


