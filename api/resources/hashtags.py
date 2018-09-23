from flask_restful import Resource
from api.db.db import DB

class Hashtags(Resource):
    def __init__(self, **kwargs):
        self.connection = kwargs['connection']

    def get(self):
        sql = """
            SELECT hashtag , COUNT(*) as count 
            FROM twitter.hashtags th  
            WHERE tweet_timestamp >= (current_timestamp - interval '10 minute')  
            GROUP BY hashtag 
            ORDER BY count DESC 
            LIMIT 10
        """

        response = {}

        try:
            db = DB(self.connection)
            rows = db.get_rows(sql)
            hashtags = []

            for row in rows:
                hashtags.append({
                    'hashtag': row[0],
                    'count': row[1]
                })

            response['hashtags'] = hashtags
            response['count'] = len(hashtags)
        except:
            response = {'result': "error"}

        return response


