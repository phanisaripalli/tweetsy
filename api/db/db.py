import pymongo
import os, sys


curr_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(curr_dir + '/../../')

from api.common import common

# db = client.get_database('twitter')


class DB():

    def __init__(self, connection):
        self.connection = connection

    def __get_client(self):
        mongo_cntn_str = common.get_mongo_connection_str()
        client = pymongo.MongoClient(mongo_cntn_str)

        return client


    def get_row(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        row = cur.fetchone()

        self.connection.commit()
        cur.close()

        return row

    def get_rows(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()


        self.connection.commit()
        cur.close()

        return rows

