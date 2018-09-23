from flask_restful import Resource
from api.t_api.api import API

class Trends(Resource):
    def __init__(self, **kwargs):
        self.twitter_api = kwargs['twitter_api']

    def get(self):


        api = API(self.twitter_api)

        trends = api.get_trends()

        json_results = [trend.AsDict() for trend in trends]
        results = []

        for result in json_results:
            if 'tweet_volume' in result:

                results.append({
                    'name': result['name'],
                    'volume': result['tweet_volume'],
                    'url': result['url']
                })


        return results


