from sqlalchemy import create_engine
from ..spark.constants import Constants
from ..logger.logger import Logger
import psycopg2

class Database(object):


    

    def getPostgresConn(self):
        self.conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(Constants.POSTGRESQL_USER,\
            Constants.POSTGRESQL_PASSWORD,Constants.POSTGRESQL_HOST_IP,\
            Constants.POSTGRESQL_PORT,Constants.POSTGRESQL_DATABASE),echo=False)


        return self.conn

    def getDbConn(self):
        self.conn = psycopg2.connect(user=Constants.POSTGRESQL_USER,
                                    password=Constants.POSTGRESQL_PASSWORD,
                                    host=Constants.POSTGRESQL_HOST_IP,
                                    port=Constants.POSTGRESQL_PORT,
                                    database=Constants.POSTGRESQL_DATABASE)


        return self.conn        