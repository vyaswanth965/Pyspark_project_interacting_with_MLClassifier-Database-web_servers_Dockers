import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
import os

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class CanonicalGenerator(object):


    def createDictfromMapfile(self,fileName):
        mappingdata_abs_path=os.path.abspath(Constants.MAPPINGS_DIRECTORY)
        mapDataFrame = pandas.read_excel('{}/{}'.format(mappingdata_abs_path,fileName),header=None)
        mapDataFrame = mapDataFrame[1:]
        mapDict = {}
        for row in range(len(mapDataFrame)):
            for column in range(mapDataFrame.iloc[row,1]):
                column = column+2
                mapDict[mapDataFrame.iloc[row,column]] = mapDataFrame.iloc[row,0]
        return mapDict       


    def generateCanonicalName(self,dataFrame,filename):
        logger.info('in generateCanonicalName')
        mapDict = self.createDictfromMapfile(filename)
        def gen(text):
            Manufacturer = mapDict.get(text,-1)
            if Manufacturer != -1:
                return Manufacturer
            else:
                return 'UNKNOWN'
        udf = Functions.UserDefinedFunction(lambda x: gen(x))   

        localDf = dataFrame.groupBy(Functions.col("Cleaned Manufacturer").alias('Cleaned Manufacturer2')).agg(Functions.lit('1'))

        localDf = localDf.withColumn("Canonical_Manufacturer", udf(Functions.col("Cleaned Manufacturer2"))).drop('1')
        dataFrame = dataFrame.join(localDf,(dataFrame['Cleaned Manufacturer'] == localDf['Cleaned Manufacturer2'])).drop(localDf['Cleaned Manufacturer2'])
    
        return dataFrame

      