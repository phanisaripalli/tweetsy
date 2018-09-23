import configparser
import os

script_dir = os.path.dirname(__file__)
connections_cfg_path = os.path.join(script_dir, 'connections.cfg')
parser = configparser.ConfigParser()

def get_mongo_connection_str():
    parser.read(connections_cfg_path)
    connection_str = parser.get('mongo_db', 'connection_str')

    return connection_str


def get_twiiter_credentials():
    parser.read(connections_cfg_path)

    consumer_key = parser.get('twitter', 'consumer_key')
    consumer_secret = parser.get('twitter', 'consumer_secret')
    access_key = parser.get('twitter', 'access_key')
    access_secret = parser.get('twitter', 'access_secret')

    return consumer_key, consumer_secret, access_key, access_secret


def get_postgres_connection_str():
    parser.read(connections_cfg_path)
    connection_str = parser.get('postgres_db', 'connection_str')

    return connection_str