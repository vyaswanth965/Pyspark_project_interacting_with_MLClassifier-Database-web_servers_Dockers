import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
import os
from pyspark.sql.types import *
from fuzzywuzzy import process,fuzz
from .lavDemo import MatchString

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

class PriorityDrugGenerator(object):

    def getdata(self,sparkSession,fileName):
        mappingdata_abs_path=os.path.abspath(Constants.MAPPINGS_DIRECTORY)
        mapDataFrame = pandas.read_excel('{}/{}'.format(mappingdata_abs_path,fileName))
        mapDataFrame = mapDataFrame.iloc[:,0:9]

        return mapDataFrame   


    def generatePriorityDrugsInfo(self,sparkSession,dataFrame,fileName,matchingThreshold,matching_type):
        logger.info('in generatePriorityDrugsInfo ')

        priorityDF = self.getdata(sparkSession,fileName)

        if matching_type == 'FUZZY': 
            cleandesc_list = priorityDF['Clean Description'].unique().tolist()
            
            def genpriorityfields(Item_description):
                bestMatch = process.extractOne(Item_description, cleandesc_list,scorer=fuzz.token_sort_ratio,score_cutoff=matchingThreshold)           

                if bestMatch is not None:
                    priority_desc = priorityDF.loc[priorityDF['Clean Description']==bestMatch[0],'Item Description'].iloc[0]
                    return '{}!{}!{}'.format('true',priority_desc,bestMatch[1])
                else:
                    return '{}!{}!{}'.format('false','','')
        else:
            cleandesc_list = priorityDF['processed clean desc'].unique().tolist()
            lav = MatchString()

            def genpriorityfields(Item_description):
                bestMatch = lav.extractOneWithcutoff(Item_description, cleandesc_list,score_cutoff=matchingThreshold)           

                if bestMatch is not None:
                    priority_desc = priorityDF.loc[priorityDF['processed clean desc']==bestMatch[0],'Item Description'].iloc[0]
                    return '{}!{}!{}'.format('true',priority_desc,bestMatch[1])
                else:
                    return '{}!{}!{}'.format('false','','')

        localDf = dataFrame.groupBy(Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))
                
        udf = Functions.UserDefinedFunction(lambda x: genpriorityfields(x))   

        localDf = localDf.withColumn("priority-fields", udf(Functions.col("Item_Description2"))).drop('1')

        localDf = dataFrame.join(localDf,(dataFrame['Item_Description'] == localDf['Item_Description2'])).drop(localDf['Item_Description2'])

        split_col = Functions.split(localDf['priority-fields'],'!')
        localDf = localDf.withColumn("Priority Drug Match Flag", split_col.getItem(0))\
                    .withColumn("Priority Drug Accuracy", split_col.getItem(2))  \
                    .withColumn("Priority Drug Desc", split_col.getItem(1)).drop('priority-fields')
        

        localDf = localDf.withColumn("tag",  Functions.when((localDf.tag == "") & (Functions.col('Priority Drug Match Flag') =='true') , "Drug").otherwise(localDf.tag))

        return localDf



    