from flask_restful import Resource
from flask_restful import request
from api.db.db import DB

class Distribution(Resource):
    def __init__(self, **kwargs):
        self.connection = kwargs['connection']

    def get(self):

        d_type = request.args.get('type')

        response = self.__get_results(d_type)

        return response


    def __get_results(self, type):
        db = DB(self.connection)

        print(type)

        if type == 'minute':
            sql = """
                SELECT to_char((t.tweet->>'tweet_timestamp')::timestamp, 'HH24:MI') as hr_min, 
                  count(*) as total_tweets 
                FROM twitter.tweets t 
                WHERE 1 = 1 
                  AND (t.tweet->>'tweet_timestamp')::timestamp >= current_timestamp - interval '10 minute' 
                GROUP BY hr_min 
                ORDER BY hr_min
            """

            rows = db.get_rows(sql)

            response = {'distibution': []}

            for row in rows:
                response['distibution'].append({
                    'hr_min': row[0],
                    'total_tweets': row[1],
                })

            return response
        else:
            sql = """
            SELECT AVG(total_tweets)::INT AS avg_tweets 
            FROM (   
              SELECT to_char((t.tweet->>'tweet_timestamp')::timestamp, 'HH24:MI') as hr_min, count(*) as total_tweets 
              FROM twitter.tweets t
              WHERE 1 = 1 
                AND (t.tweet->>'tweet_timestamp')::timestamp >= current_timestamp - interval '10 minute' 
              GROUP BY hr_min 
              ORDER BY hr_min 
            ) sq 
            """

            response = {'avg_tweets': None, 'has_result': False}
            row = db.get_row(sql)

            if row:
                response['avg_tweets'] = row[0]
                response['has_result'] = True

            return response
