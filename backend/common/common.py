import configparser
import os


script_dir = os.path.dirname(__file__)
connections_cfg_path = os.path.join(script_dir, 'connections.cfg')
parser = configparser.ConfigParser()

    
def get_mongo_connection_str():    
    parser.read(connections_cfg_path)
    connection_str = parser.get('mongo_db', 'connection_str')
    
    return connection_str


