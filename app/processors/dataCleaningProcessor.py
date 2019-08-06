import pyspark.sql.functions as Functions
from framework.logger.logger import Logger 

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class DataCleaner(object):


    # Remove special characters, trimming and normalization of process
    def cleanText(self, text):
        text = Functions.trim(text)
        text = Functions.lower(text)
        text = Functions.regexp_replace(text,'[\\,,\\@,\\.]','')
        text = Functions.regexp_replace(text,'\\s+',' ')  
        text = Functions.trim(text)
        return text
    
    # Remove stop wards in the name of manufactorer
    def manufacturerStopWords(self, text):
        text = Functions.regexp_replace(text, r' t/t$', '')
        text = Functions.regexp_replace(text, r' llc$', '')
        text = Functions.regexp_replace(text, r' inc$', '')
        text = Functions.regexp_replace(text, r' usa$', '')
        text = Functions.regexp_replace(text, r' sa$', '')
        text = Functions.regexp_replace(text, r' corporation$', '')
        text = Functions.regexp_replace(text, r' corp$', '')
        text = Functions.regexp_replace(text, r' co$', '')
        text = Functions.regexp_replace(text, r' incorporated$', '')
        text = Functions.regexp_replace(text, r' i$', '')
        text = Functions.trim(text)
        return text

    def cleanDataFrame(self, dataFrame):
        logger.info('in cleanDataFrame')

        dataFrame = dataFrame.withColumn('Cleaned Item Description', \
            Functions.lower(Functions.trim(Functions.regexp_replace(Functions.col('Item_Description'),'\\@',''))))

        dataFrame = dataFrame.withColumn('Cleaned Manufacturer', self.cleanText(Functions.col('Manufacturer')))
        dataFrame = dataFrame.withColumn('Cleaned Manufacturer', self.manufacturerStopWords(Functions.col('Cleaned Manufacturer')))

        dataFrame = dataFrame.withColumn('Cleaned Item Number ', \
            Functions.regexp_replace((Functions.trim(Functions.col('Item Number '))),'^0+(?!$)', ''))

        return dataFrame