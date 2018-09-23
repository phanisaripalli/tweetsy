from flask import Flask
from flask_restful import Api
import twitter
import psycopg2

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from api.common import common
from api.resources.test import Test
from api.resources.tweets import Tweets
from api.resources.trends import Trends
from api.resources.overview import Overview
from api.resources.hashtags import Hashtags
from api.resources.distribution import Distribution

consumer_key, consumer_secret, access_token_key, access_token_secret = common.get_twiiter_credentials()
twitter_api = twitter.Api(consumer_key=consumer_key,
                          consumer_secret=consumer_secret,
                          access_token_key=access_token_key,
                          access_token_secret=access_token_secret
                          )
# import pymongo
# mongo_cntn_str = common.get_mongo_connection_str()
# client = pymongo.MongoClient(mongo_cntn_str)

pg_cntn_str = common.get_postgres_connection_str()
pg_connection = psycopg2.connect((pg_cntn_str))


app = Flask(__name__)
app.config['DEBUG'] = True
app.config['JSON_AS_ASCII'] = False
api = Api(app)

from flask_cors import CORS
cors = CORS(app, resources={r"/*": {"origins": "*"}})


api.add_resource(Test,
                 '/test',
                 '/test/',
                 endpoint='test',
                 strict_slashes=False
)

api.add_resource(Tweets,
                 '/tweets',
                 endpoint='tweets',
                 resource_class_kwargs={
                     'twitter_api': twitter_api
                 },
                 strict_slashes=False
                 )

api.add_resource(Trends,
                 '/trends',
                 endpoint='trends',
                 resource_class_kwargs={
                     'twitter_api': twitter_api
                 },
                 strict_slashes=False
                 )

api.add_resource(Overview,
                 '/overview',
                 endpoint='overview',
                 resource_class_kwargs={
                     'connection': pg_connection
                 },
                 strict_slashes=False
                 )

api.add_resource(Hashtags,
                 '/hashtags',
                 endpoint='hashtags',
                 resource_class_kwargs={
                     'connection': pg_connection
                 },
                 strict_slashes=False
                 )

api.add_resource(Distribution,
                 '/distribution',
                 endpoint='distribution',
                 resource_class_kwargs={
                     'connection': pg_connection
                 },
                 strict_slashes=False
                 )


if __name__ == '__main__':
    app.run()
