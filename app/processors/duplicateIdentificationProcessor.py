import pyspark.sql.functions as Functions
from pyspark.sql.window import Window
from framework.logger.logger import Logger 

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class DuplicateIdentifier(object):


    def constructKey(self, dataFrame):
        localDf = dataFrame.withColumn('Key', \
            Functions.concat(Functions.col('Cleaned Item Number '),Functions.lit('#'),\
            Functions.col('Cleaned Manufacturer'),Functions.lit('#'),Functions.col('Cleaned Item Description')))
        localDf = localDf.withColumn('Key', \
            Functions.regexp_replace(Functions.col('Key'),'\\s+','_'))
        return localDf

    def markDistinct(self, dataFrame):
        w = Window().partitionBy('Key').orderBy(Functions.lit('A'))
        localDf = dataFrame.withColumn('IsDistinctKey', Functions.row_number().over(w))
        localDf = localDf.withColumn('IsDistinctKey',Functions.when(localDf.IsDistinctKey==1,'0').otherwise('1'))
        return localDf

    def markDuplicate(self, dataFrame):
        logger.info('in markDuplicate')
        dataFrame = self.constructKey(dataFrame = dataFrame)
        dataFrame = self.markDistinct(dataFrame = dataFrame)

        return dataFrame