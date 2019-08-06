import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
from framework.datasource.docker import DockerClient
from pyspark.sql.types import *
import os
import docker
import requests
import xml.etree.ElementTree as ET 

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class RxInfoGenerator(object):
    log = None
    container1 = None
    container2 = None

    def __init__(self):
        dockerClient = DockerClient()
        self.container1 = dockerClient.startContainer('tomcat-box')
        self.container2 = dockerClient.startContainer('mariadb-box')

    def generateRxInfo(self,dataFrame):

        logger.info('in generateRxInfo')
        base_uri = 'http://{}:{}/REST/rxcui/'.format(Constants.POSTGRESQL_HOST_IP,Constants.RXNAV_PORT)
        
        def genRxInfo(RxNormId):
            d={'BN':[],'IN':[],'tag':set()}
            if RxNormId !='':
                try:
                    resp = requests.get(url=base_uri+RxNormId+"/allrelatedextension")
                    root = ET.fromstring(resp.content)
                    for conceptGroup in root.findall('./allRelatedGroup/conceptGroup'):
                        for tty in conceptGroup.findall('./tty'):
                            if tty.text=='BN':
                                for brandname in conceptGroup.findall('./conceptProperties/name'):
                                        d['BN'].append(brandname.text)
                            if tty.text=='IN':
                                for Ingredientname in conceptGroup.findall('./conceptProperties/name'):
                                        d['IN'].append(Ingredientname.text)
                                for humandrug in conceptGroup.findall('./conceptProperties/inferedhuman'):
                                    if humandrug.text=='US':
                                            d['tag'].add('Human_Drug')
                                for animaldrug in conceptGroup.findall('./conceptProperties/inferedvet'):
                                    if animaldrug.text=='US':
                                            d['tag'].add('Animal_Drug')
                except ET.ParseError as e:
                    print("Invalid XML received from uri {}".format(e))                                                      
            return str(d['BN'])+'|'+str(d['IN'])+'|'+','.join(d['tag'])
  
        udf = Functions.UserDefinedFunction(genRxInfo, StringType())
        
        localDf = dataFrame.groupBy(Functions.col("RxNormId").alias("RxNormId2")).agg(Functions.lit('1'))

        localDf = localDf.withColumn("RxInfo",udf(Functions.col("RxNormId2"))).drop('1')

        localDf = dataFrame.join(localDf,(dataFrame['RxNormId'] == localDf['RxNormId2'])).drop(localDf['RxNormId2'])

        split_col = Functions.split(localDf['RxInfo'],'\|')
        localDf = localDf.withColumn("Brand_Names", split_col.getItem(0))\
                        .withColumn("Ingredients", split_col.getItem(1)) \
                        .withColumn("tag",Functions.when(localDf.tag== '', split_col.getItem(2))\
                            .otherwise(Functions.when(Functions.length( split_col.getItem(2))==0,localDf.tag)\
                                .otherwise(Functions.concat(localDf.tag,Functions.lit(','),split_col.getItem(2))))).drop('RxInfo')       


        #self.container1.stop()
        #self.container2.stop()
        return localDf






