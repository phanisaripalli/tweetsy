from flask_restful import Resource
from api.db.db import DB

class Overview(Resource):

    def __init__(self, **kwargs):
        self.connection = kwargs['connection']

    def get(self):


        sql = """            
            SELECT t.tweet->>'search_key', count(*) as total_tweets, 
                count(distinct(t.tweet->>'user_id')) as distinct_users, 
                min((t.tweet->>'tweet_timestamp')::timestamp)::text as min_recorded
            FROM twitter.tweets t                      
            WHERE 1 = 1 
                AND (t.tweet->>'tweet_timestamp')::timestamp >= 
                    (current_timestamp - interval '10 minute')
            GROUP BY t.tweet->>'search_key'
            ORDER BY total_tweets DESC
            LIMIT 1
        """

        response = {'has_result': False}

        try:

            db = DB(self.connection)

            row = db.get_row(sql)

            if row:

                response['result'] = 'success'
                response['search_key'] = row[0]
                response['total_tweets'] = row[1]
                response['distinct_users'] = row[2]
                response['min_timestamp'] = row[3]
                response['has_result'] = True

        except Exception as e:
            print(str(e))
            response = {'result': 'error', 'has_result': False}


        return response


