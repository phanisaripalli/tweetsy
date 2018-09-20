from flask_restful import Resource
from flask_restful import request
from api.db.db import DB

class Trends(Resource):
    def __init__(self, **kwargs):
        self.client = kwargs['client']
        self.twitter_api = kwargs['twitter_api']

    def get(self):


        db = DB(self.twitter_api, self.client)

        trends = db.get_trends()

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


