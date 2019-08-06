import pandas
from pyspark.sql.types import *
from .constants import Constants
from ..logger.logger import Logger 
import xlrd
import os

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class DataReader():


    def ReadData(self, sparkSession):
        pandasDataFrame=pandas.DataFrame()

        for file in os.listdir(Constants.DATA_DIRECTORY):
            try:
                dataFrame = pandas.read_excel('{}/{}'.format(Constants.DATA_DIRECTORY,file),\
                    dtype={'Item Description':str, 'Item Number ':str,'Manufacturer':str},header=None)
                logger.info('reading file {}/{}'.format(Constants.DATA_DIRECTORY,file))
                dataFrame = dataFrame.drop(dataFrame.index[0])

            except xlrd.biffh.XLRDError as e:
                logger.error('unsupported file {}/{} {}'.format(Constants.DATA_DIRECTORY,file,e))

            pandasDataFrame=pandasDataFrame.append(dataFrame,sort=False)
        '''data_abs_path=os.path.abspath(Constants.DATA_DIRECTORY)
        pandasDataFrame = pandas.read_excel('{}/{}'.format(data_abs_path,'100.xlsx')\
        ,dtype={'Item_Description':str, 'Item Number ':str,'Manufacturer':str})'''
        pandasDataFrame.fillna('',inplace=True)
        logger.info('reading completed')
        schema = StructType([ StructField('Item_Description', StringType(), True)
                                ,StructField('Item Number ', StringType(), True)
                                ,StructField('Manufacturer', StringType(), True)])

        sparkDataFrame = sparkSession.createDataFrame(pandasDataFrame,schema=schema)
        
        pandasDataFrame = None
        return sparkDataFrame