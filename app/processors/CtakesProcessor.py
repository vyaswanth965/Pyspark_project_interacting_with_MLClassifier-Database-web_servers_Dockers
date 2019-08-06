import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
from framework.datasource.docker import DockerClient
from pyspark.sql.types import *
import os
import docker
import requests

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class RxNormGenerator(object):
    DOCKER_CTAKE_CONTAINER_NAME = 'ctakes-box'

    log = None
    container = None

    def __init__(self):
        dockerClient = DockerClient()
        self.container = dockerClient.startContainer(self.DOCKER_CTAKE_CONTAINER_NAME)

    def generateRxNormId(self,dataFrame):
        logger.info('in generateRxNormId')
        base_uri = 'http://{}:{}/process/rxnorm?q='.format(Constants.POSTGRESQL_HOST_IP,Constants.CTAKES_PORT)
        
        def genRxNormId(drugName):
            resp = requests.get(url=base_uri+drugName)
            data = resp.json()
            if len(data)>0:
                return data[0]['code']                
            else:
                return ''    

        udf = Functions.UserDefinedFunction(genRxNormId, StringType())

        localDf = dataFrame.groupBy(Functions.col("Drug_Name").alias('Drug_Name2')).agg(Functions.lit('1'))

        localDf = localDf.withColumn("RxNormId",udf(Functions.col("Drug_Name2"))).drop('1')
        
        localDf = dataFrame.join(localDf,(dataFrame['Drug_Name'] == localDf['Drug_Name2'])).drop(localDf['Drug_Name2'])

        localDf = localDf.withColumn("tag",  Functions.when((localDf.tag == "") & (localDf.RxNormId != "") , "Drug").otherwise(localDf.tag))

        return localDf






