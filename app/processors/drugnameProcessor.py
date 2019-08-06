import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
import os

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class DrugNameGenerator(object):


    def createDictFromKeywords(self,fileName):
        mappingdata_abs_path=os.path.abspath(Constants.MAPPINGS_DIRECTORY)
        keywordsDataFrame=pandas.read_excel('{}/{}'.format(mappingdata_abs_path,fileName),header=None)
        keywordsDict={}
        for i in range(len(keywordsDataFrame)):
            keywordsDict[keywordsDataFrame.iloc[i,0].lower()]=keywordsDataFrame.iloc[i,1].lower()
        return keywordsDict

    def generateDrugNames(self,dataFrame,filename):
        logger.info('in generateDrugNames')
        keywordsDict = self.createDictFromKeywords(filename)  
        split_col = Functions.split(dataFrame['Cleaned Item Description'],'(?<=^[^\\s]*)\\s')
        dataFrame = dataFrame.withColumn("Drug_Name", Functions.regexp_replace(split_col.getItem(0),'[\\,,\\.]',''))
        dataFrame = dataFrame.withColumn("Drug Strength", split_col.getItem(1))

        def genDosageDict(text):
            tokens = text.split()
            dosage_fullname = ''
            dosage = ''
            tag = ''
            keywords = []

            for token in tokens:
                token_fullname = keywordsDict.get(token,-1)
                if token_fullname != -1:
                    keywords.append(token_fullname)
                    if len(keywords) == 1:
                        dosage_fullname = token_fullname
                        dosage = token
                        tag = 'Drug'

            keywords = set(keywords)
            if len(keywords) > 0:
                return '{}!{}!{}!{}'.format(dosage,dosage_fullname,tag,keywords)
            else:
                return '{}!{}!{}!{}'.format(dosage,dosage_fullname,tag,'')


        udf = Functions.UserDefinedFunction(lambda x: genDosageDict(x))   
        localDf = dataFrame.groupBy(Functions.col("Cleaned Item Description").alias('Cleaned Item Description2')  ).agg(Functions.lit('1'))

        localDf = localDf.withColumn("dosage-fields", udf(Functions.col("Cleaned Item Description2"))).drop('1')

        split_col = Functions.split(localDf['dosage-fields'],'!')
        localDf = localDf.withColumn("dosage", split_col.getItem(0))\
                    .withColumn("dosage_fullname", split_col.getItem(1))  \
                    .withColumn("tag", split_col.getItem(2))\
                    .withColumn("keywords", split_col.getItem(3)).drop('dosage-fields')

        dataFrame = dataFrame.join(localDf,(dataFrame['Cleaned Item Description'] == localDf['Cleaned Item Description2'])).drop(localDf['Cleaned Item Description2'])

        dataFrame = dataFrame.select("Item_Description","Item Number ", "Manufacturer","Canonical_Manufacturer"\
                    ,"Drug_Name","tag","Key",Functions.col("IsDistinctKey").alias('Excluded'))
        

        return dataFrame