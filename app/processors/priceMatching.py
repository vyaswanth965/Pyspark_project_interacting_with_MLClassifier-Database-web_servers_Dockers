import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
import os
from sqlalchemy import create_engine
from fuzzywuzzy import process,fuzz
from pyspark.sql.types import *
import shutil
import sys
from framework.datasource.database import Database
import psycopg2
import pandas.io.sql as sqlio
import pickle
from psycopg2.extras import NamedTupleCursor
import datetime
from processors.dataCleaningProcessor import DataCleaner
from .lavDemo import MatchString

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class PriceInfoGenerator(object):

    def common_process(self,proddesc_list,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,listType):
        lav = MatchString()

        if len(proddesc_list)==0:
            return '|||||||||||||||||||||'
            
        if shouldConsiderOnlyTopScore:
            bestMatch = lav.extractBestMatches(Item_Description,proddesc_list,limit=1,score_cutoff=matchingThreshold)

            if len(bestMatch) == 0:
                return '|||||||||||||||||||||'
            else:
                return self.getmatchedrecord(bestMatch[0][0],bestMatch[0][1],cursor,listType)
        else:
            bestMatches = lav.extractBestMatches(Item_Description,proddesc_list,limit=5,score_cutoff=matchingThreshold)
            if len(bestMatches)==0:
                return '|||||||||||||||||||||'
            else:
                bestMatches = set(bestMatches)
                rtrn_str=''
                for match in bestMatches:     
                    matched_record = self.getmatchedrecord(match[0],match[1],cursor,listType)
                    rtrn_str = rtrn_str+'#'+matched_record
                rtrn_str.strip('#')
                return rtrn_str


    def common_process2(self,local_df):
        local_df = local_df.withColumn('priceinfo',Functions.explode(Functions.split(Functions.col("priceinfo"), "#")))
        split_col = Functions.split(local_df['priceinfo'],'\|')
        local_df = local_df.withColumn("Matched Pricing Item", split_col.getItem(0))\
                        .withColumn("UOM", split_col.getItem(1)) \
                        .withColumn("Number of Orders", split_col.getItem(2)) \
                        .withColumn("Total Quantity", Functions.when( split_col.getItem(12)!='', split_col.getItem(12)).otherwise(split_col.getItem(3)))\
                        .withColumn("Min Price Amount", Functions.when( split_col.getItem(13)!='', split_col.getItem(13)).otherwise(split_col.getItem(4)))\
                        .withColumn("Max Price Amount", Functions.when( split_col.getItem(14)!='', split_col.getItem(14)).otherwise(split_col.getItem(5)))\
                        .withColumn("Mean Price Amount", Functions.when( split_col.getItem(15)!='', split_col.getItem(15)).otherwise(split_col.getItem(6)))\
                        .withColumn("Stddev Price Amount",Functions.when( split_col.getItem(16)!='', split_col.getItem(16)).otherwise( split_col.getItem(7)))\
                        .withColumn("Q1 Price Amount", Functions.when( split_col.getItem(17)!='', split_col.getItem(17)).otherwise(split_col.getItem(8)))\
                        .withColumn("Q2 Price Amount",Functions.when( split_col.getItem(18)!='', split_col.getItem(18)).otherwise( split_col.getItem(9)))\
                        .withColumn("Q3 Price Amount",Functions.when( split_col.getItem(19)!='', split_col.getItem(19)).otherwise( split_col.getItem(10)))\
                        .withColumn("Q4 Price Amount",Functions.when( split_col.getItem(20)!='', split_col.getItem(20)).otherwise( split_col.getItem(11)))\
                        .withColumn("Matched Pricing Accuracy", split_col.getItem(21)).drop('priceinfo')


        return local_df                               


    def common_process3(self,local_df):
        local_df = local_df.withColumn('priceinfo',Functions.explode(Functions.split(Functions.col("priceinfo"), "#")))
        split_col = Functions.split(local_df['priceinfo'],'\|')
        local_df = local_df.withColumn("itemdesc", split_col.getItem(0))\
                        .withColumn("quantity", split_col.getItem(1)) \
                        .withColumn("manufacturer", split_col.getItem(2)) \
                        .withColumn("pricepaid", split_col.getItem(3))\
                        .withColumn("totalspend", split_col.getItem(4))\
                        .withColumn("UOM", split_col.getItem(5))\
                        .withColumn("score", split_col.getItem(6)).drop('priceinfo')

        return local_df 

    def getmatchedrecord(self,bestmatch,score,cursor,listType):
        if listType=='prodDesc':
            postgreSQL_select_Query = "select prodDesc,uom,totalnumberoforders,\
                    totalnumberofunitspurchasedpo,minpricepo,maxpricepo,avgpricepo,COALESCE(stdevpricepo,0) stdevpricepo,\
                    percentile25thpricepo,percentile50thpricepo,percentile75thpricepo,percentile99thpricepo,\
                    totalNumberofunitspurchasedinvoice,minpriceinvoice,maxpriceinvoice,avgpriceinvoice,\
                    COALESCE(stdevpriceinvoice,0) stdevpriceinvoice,percentile25thpriceinvoice,percentile50thpriceinvoice,\
                    percentile75thpriceinvoice,percentile99thpriceinvoice from priceinfo350k where id={}".format(bestmatch)
        elif listType=='itemDesc':
            postgreSQL_select_Query = "select itemdesc,quantity,manufacturer,pricepaid,totalspend,uom from priceinfo9july2019 \
                where id={}".format(bestmatch)         
        cursor.execute(postgreSQL_select_Query)
        record = cursor.fetchone()
        recordstr = '|'.join(str(i) for i in record)
        return recordstr+'|'+str(score)



    
    
    def generateSpendInfoBasedOnUnspsc(self,sparkContext,sparkSession,DataFrame,shouldConsiderOnlyTopScore,matchingThreshold):
        def getPriceINfo(Unspsc_Code,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold):
            db = Database()
            connection = db.getDbConn()

            cursor = connection.cursor()

            postgreSQL_select_Query = "select prodcleandesc,id from uniqueProducts_UNSPSC where unspscclasscode={}".format(Unspsc_Code)

            cursor.execute(postgreSQL_select_Query)

            records = cursor.fetchall()

            result = self.common_process(records,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,'prodDesc')
            cursor.close()
            connection.close()
            return result

           
                              
        localDf = DataFrame.groupBy(Functions.col("Unspsc_Code").alias('Unspsc_Code2'),Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))
           
                              
        udf = Functions.UserDefinedFunction(getPriceINfo, StringType())

        localDf = localDf.withColumn('priceinfo',udf(Functions.col('Unspsc_Code2'),Functions.col('Item_Description2'),\
            Functions.lit(shouldConsiderOnlyTopScore),Functions.lit(matchingThreshold))).drop('1')        

        DataFrame = DataFrame.join(localDf,(localDf.Unspsc_Code2 == DataFrame.Unspsc_Code) & \
            (localDf.Item_Description2 == DataFrame.Item_Description)).drop(localDf.Item_Description2).drop(localDf.Unspsc_Code2)

        DataFrame = self.common_process2(DataFrame)


        return DataFrame
   

    
    def generateSpendInfoBasedOnManufacturer(self,sparkContext,sparkSession,DataFrame,shouldConsiderOnlyTopScore,matchingThreshold):
        def getPriceINfo(Canonical_Manufacturer,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold):
            db = Database()
            connection = db.getDbConn()

            cursor = connection.cursor()

            postgreSQL_select_Query = "select prodcleandesc,id from uniqueProducts where lower(mfrcanonicalname)=\'{}\'".format(Canonical_Manufacturer)
            
            cursor.execute(postgreSQL_select_Query)

            records = cursor.fetchall()

            result = self.common_process(records,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,'prodDesc')

            cursor.close()
            connection.close()
            return result

                             
        localDf = DataFrame.groupBy(Functions.col("Canonical_Manufacturer").alias('Canonical_Manufacturer2'),Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))
                             
        udf = Functions.UserDefinedFunction(getPriceINfo, StringType())

 
        localDf = localDf.withColumn('priceinfo',udf(Functions.col('Canonical_Manufacturer2'),Functions.col('Item_Description2'),\
            Functions.lit(shouldConsiderOnlyTopScore),Functions.lit(matchingThreshold))).drop('1')       

        DataFrame = DataFrame.join(localDf,(localDf.Canonical_Manufacturer2 == DataFrame.Canonical_Manufacturer) & \
            (localDf.Item_Description2 == DataFrame.Item_Description)).drop(localDf.Item_Description2).drop(localDf.Canonical_Manufacturer2)


        DataFrame = self.common_process2(DataFrame)

        return DataFrame        


    
    

    def generateNewSpendInfoBasedOnDrug(self,sparkContext,sparkSession,DataFrame,shouldConsiderOnlyTopScore,matchingThreshold):
        def getPriceINfo(Drug_Name,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold):
            db = Database()
            connection = db.getDbConn()

            cursor = connection.cursor()

            postgreSQL_select_Query = "select itemdesc,id from priceinfo9july2019 where lower(itemdesc) like \'{}\'".format(Drug_Name)
            
            cursor.execute(postgreSQL_select_Query)

            records = cursor.fetchall()

            result = self.common_process(records,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,'itemDesc')

            cursor.close()
            connection.close()
            return result

                             
        udf = Functions.UserDefinedFunction(getPriceINfo, StringType())
        
        localDf = DataFrame.groupBy(Functions.col("Drug_Name").alias('Drug_Name2'),Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))
 
        localDf = localDf.withColumn('priceinfo',udf(Functions.col('Drug_Name2'),Functions.col('Item_Description2'),\
            Functions.lit(shouldConsiderOnlyTopScore),Functions.lit(matchingThreshold))).drop('1')       

        DataFrame = DataFrame.join(localDf,(localDf.Drug_Name2 == DataFrame.Drug_Name) & \
            (localDf.Item_Description2 == DataFrame.Item_Description)).drop(localDf.Item_Description2).drop(localDf.Drug_Name2)

        DataFrame = self.common_process3(DataFrame)

        return DataFrame


    
    
    def generateNewSpendInfoBasedOnManu(self,sparkContext,sparkSession,DataFrame,shouldConsiderOnlyTopScore,matchingThreshold):
        def getPriceINfo(Canonical_Manufacturer,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold):
            db = Database()
            connection = db.getDbConn()

            cursor = connection.cursor()

            postgreSQL_select_Query = "select itemdesc,id from priceinfo9july2019 where lower(mfrcanonicalname) like \'{}\'".format(Canonical_Manufacturer)
            
            cursor.execute(postgreSQL_select_Query)

            records = cursor.fetchall()

            result = self.common_process(records,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,'itemDesc')

            cursor.close()
            connection.close()
            return result

                             
        udf = Functions.UserDefinedFunction(getPriceINfo, StringType())

        localDf = DataFrame.groupBy(Functions.col("Canonical_Manufacturer").alias('Canonical_Manufacturer2'),Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))

        localDf = localDf.withColumn('priceinfo',udf(Functions.col('Canonical_Manufacturer2'),Functions.col('Item_Description2'),\
            Functions.lit(shouldConsiderOnlyTopScore),Functions.lit(matchingThreshold))).drop('1')       

        DataFrame = DataFrame.join(localDf,(localDf.Canonical_Manufacturer2 ==DataFrame.Canonical_Manufacturer) & \
            (localDf.Item_Description2 == DataFrame.Item_Description)).drop(localDf.Item_Description2).drop(localDf.Canonical_Manufacturer2)


        DataFrame = self.common_process3(DataFrame)

        return DataFrame          


    
    
    def generateNewSpendInfoBasedOnDescription(self,sparkContext,sparkSession,DataFrame,shouldConsiderOnlyTopScore,matchingThreshold):
        def getPriceINfo(Item_Description,shouldConsiderOnlyTopScore,matchingThreshold):
            db = Database()
            connection = db.getDbConn()

            cursor = connection.cursor()

            postgreSQL_select_Query = "select itemdesc,id from priceinfo9july2019 where lower(itemdesc) like \'{}\'".format(Item_Description)
            
            cursor.execute(postgreSQL_select_Query)

            records = cursor.fetchall()

            result = self.common_process(records,Item_Description,shouldConsiderOnlyTopScore,matchingThreshold,cursor,'itemDesc')

            cursor.close()
            connection.close()
            return result

                             
        udf = Functions.UserDefinedFunction(getPriceINfo, StringType())

        localDf = DataFrame.groupBy(Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))
 
        localDf = localDf.withColumn('priceinfo',udf(Functions.col('Item_Description2'),\
            Functions.lit(shouldConsiderOnlyTopScore),Functions.lit(matchingThreshold))).drop('1')      

        DataFrame = DataFrame.join(localDf,\
            localDf.Item_Description2 == DataFrame.Item_Description).drop(localDf.Item_Description2)

        DataFrame = self.common_process3(DataFrame)

        return DataFrame         
    

    
    
    
    
    
    
    def generateSpendInfo(self,sparkContext,sparkSession,dataFrame,shouldConsiderOnlyTopScore,matchingThreshold,matchingflag,projectName):
        logger.info('in generateSpendInfo')
        if matchingflag=='MANUFACTURER':
            unspscDF = dataFrame.filter(dataFrame.tag == '')
            ManuDF = dataFrame.filter(dataFrame.tag != '')

            unspscResults = self.generateSpendInfoBasedOnUnspsc(sparkContext,sparkSession, unspscDF,shouldConsiderOnlyTopScore,matchingThreshold)
            manufactResults = self.generateSpendInfoBasedOnManufacturer(sparkContext,sparkSession, ManuDF,shouldConsiderOnlyTopScore,matchingThreshold)

            results = unspscResults.union(manufactResults)
            data_abs_path=os.path.abspath(Constants.OUTPUT_LOCATION+'/'+projectName)
            results.toPandas().to_csv('{}/{}_{}_PriceInfo.csv'.format(data_abs_path,str(matchingThreshold),matchingflag),index=False)

        elif matchingflag=='UNSPSC_CODE':  
            results = self.generateSpendInfoBasedOnUnspsc(sparkContext,sparkSession, dataFrame,shouldConsiderOnlyTopScore,matchingThreshold)
            
            data_abs_path=os.path.abspath(Constants.OUTPUT_LOCATION+'/'+projectName)
            results.toPandas().to_csv('{}/{}_{}_PriceInfo.csv'.format(data_abs_path,str(matchingThreshold),matchingflag),index=False)

        elif matchingflag=='NEW_SPEND_INFO':
            drugDF = dataFrame.filter(dataFrame.Drug_Name != '')
            manuDF = dataFrame.filter((dataFrame.Drug_Name == '') & (dataFrame.Canonical_Manufacturer != ''))
            descDF = dataFrame.filter((dataFrame.Drug_Name == '') & (dataFrame.Canonical_Manufacturer == ''))
            
            drugResults = self.generateNewSpendInfoBasedOnDrug(sparkContext,sparkSession, drugDF,shouldConsiderOnlyTopScore,matchingThreshold)
            manuResults = self.generateNewSpendInfoBasedOnManu(sparkContext,sparkSession, manuDF,shouldConsiderOnlyTopScore,matchingThreshold)
            descResults = self.generateNewSpendInfoBasedOnDescription(sparkContext,sparkSession, descDF,shouldConsiderOnlyTopScore,matchingThreshold)

            results = drugResults.union(manuResults).union(descResults)

            data_abs_path=os.path.abspath(Constants.OUTPUT_LOCATION+'/'+projectName)
            results.toPandas().to_csv('{}/{}_NewPriceInfo.csv'.format(data_abs_path,str(matchingThreshold)),index=False)              



       