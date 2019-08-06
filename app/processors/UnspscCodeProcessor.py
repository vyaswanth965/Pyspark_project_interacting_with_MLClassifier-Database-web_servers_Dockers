import pyspark.sql.functions as Functions
from pyspark.sql.window import Window
from framework.logger.logger import Logger
from framework.spark.constants import Constants
import uuid
from pyspark.sql.types import *
import pandas
import docker
import time
import shutil
import pathlib
import os
import subprocess
import sys
from framework.datasource.docker import DockerClient

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class UnspscCodeGenerator(object):
    log = None
    con1 = None
    con2 = None
    def __init__(self):
        dockerClient = DockerClient()
        self.con1 = dockerClient.startContainer('postgres-imm-classifiers-box') #imm specific postgres container
        self.con2 = dockerClient.startContainer('imm-classifiers-box') #imm classifier container


    def generatePIM(self,dataFrame):
        '''w = Window().orderBy(Functions.col('Key'))
        localDf = dataFrame.withColumn('PIM', Functions.dense_rank().over(w))
        localDf =localDf.repartition(4)'''

        lookup = (dataFrame.select('Key').distinct().rdd.zipWithIndex()\
            .map(lambda x: x[0] + (x[1], )).toDF(['Key', 'PIM']))

        localDf = dataFrame.join(lookup, ['Key']).drop(lookup.Key)
        return localDf

    def createIMMClassifierInput(self, dataFrame):
        dataFrame = self.generatePIM(dataFrame)

        udf = Functions.UserDefinedFunction(lambda x: str(uuid.uuid1()))   

        localDf = dataFrame.filter(dataFrame.Excluded == '0')
        localDf = localDf.withColumn('Part Number', udf(Functions.lit('')) ) 
        localDf = localDf.select(Functions.col('PIM').alias('PIM_KEY')\
                    ,Functions.col('Canonical_Manufacturer').alias('COMPANY_NAME')\
                    ,Functions.col('Part Number').alias('PART_NUMBER')\
                    ,Functions.col('Item_Description').alias('PRODUCT_DESCRIPTION')\
                    ,Functions.lit('').alias('UNSPSC'))
        


        data_abs_path=os.path.abspath(Constants.IMM_CLASSIFIER_INPUT_FILE_LOCATION)
        localDf.toPandas().to_csv('{}/input.csv'.format(data_abs_path),index=False)
        
        logger.info("IMM classifier input.csv file placed in {} ".format(data_abs_path))

        return dataFrame    


    def startIMMCClassifier(self,dataFrame, sparkSession):

        try:
            logger.info("starting genarate_unspsc.sh ")
            status=subprocess.call([Constants.GENERATE_UNSPSC_SHELL_SCRIPT])        
        except Exception as e:
            logger.error("error in genarate_unspsc.sh ",e)
            sys.exit(1)


        if status==0:
            logger.info("predictions saved in imm-classifiers-box/data/imm-classifier-best/predictions")
            pandasDataFrame = pandas.read_csv('{}'.format(Constants.IMM_CLASSIFIER_OUTPUT_FILE))
            
            schema = StructType([ StructField('pim_key', IntegerType(), True)\
                                    ,StructField('company_name', StringType(), True)\
                                    ,StructField('part_number', StringType(), True)\
                                    ,StructField('description', StringType(), True)\
                                    ,StructField('data_set', StringType(), True)\
                                    ,StructField('first', StringType(), True)\
                                    ,StructField('secondCategory', StringType(), True)\
                                    ,StructField('thirdCategory', StringType(), True)\
                                    ,StructField('1stProb', StringType(), True)\
                                    ,StructField('2ndProb', StringType(), True)\
                                    ,StructField('3rdProb', StringType(), True)])

            predictionsDf = sparkSession.createDataFrame(pandasDataFrame,schema=schema)

            localDf = dataFrame.join(predictionsDf, dataFrame.PIM == predictionsDf.pim_key\
                ).select(dataFrame['*'],predictionsDf['first'].alias('Unspsc_Code'))

            return localDf
        else:
            logger.error('genarate_unspsc.sh processed without error but exited with status: {}'.format(status))
            sys.exit(1)

    def generateUnspscCode(self,dataFrame,sparkSession):
        logger.info('in generateUnspscCode')
        dataFrame = self.createIMMClassifierInput(dataFrame)
        return self.startIMMCClassifier(dataFrame,sparkSession)
