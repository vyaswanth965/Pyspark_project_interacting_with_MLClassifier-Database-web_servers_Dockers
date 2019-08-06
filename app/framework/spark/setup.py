from .constants import Constants
import findspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class SparkSetup():
    def setupSpark(self):
        
        sparkConfig = SparkConf() \
            .setAppName(Constants.APPLICATION_NAME) \
            .setMaster(Constants.SPARK_MASTER)\
            .set('spark.driver.memory', '12g')\
            .set('spark.executor.memory', '12g')\
            .set("spark.shuffle.service.enabled", False)\
            .set("spark.dynamicAllocation.enabled", False)\
            .set("spark.io.compression.codec", "snappy")\
            .set("spark.rdd.compress", True)\
            .set("spark.sql.shuffle.partitions", '12')




        sparkContext = SparkContext.getOrCreate( conf = sparkConfig)
        sparkSession = SparkSession.builder.appName(Constants.APPLICATION_NAME).getOrCreate()
        sparkSession.sparkContext.setLogLevel("ERROR")

        logger.info('spark setup initialized')
        return sparkContext, sparkSession