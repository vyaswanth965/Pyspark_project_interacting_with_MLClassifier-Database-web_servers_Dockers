import pyspark.sql.functions as Functions
import pandas
from framework.logger.logger import Logger
from framework.spark.constants import Constants
from framework.datasource.database import Database
from framework.datasource.docker import DockerClient
import os
from sqlalchemy import create_engine
from fuzzywuzzy import process,fuzz
from pyspark.sql.types import *
import docker
import shutil
import sys
from psycopg2.extras import NamedTupleCursor
import psycopg2

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())



class NdcGenerator(object):
    log = None
    container = None
    def __init__(self):
        dockerClient = DockerClient()
        self.container = dockerClient.startContainer('postgres-box')
    
    def readTableIntoPandasDF(self,conn,tablename):
        return pandas.read_sql('SELECT * FROM {}'.format(tablename), conn)

    def generateProprietaryNames(self,dataFrame,project_name):

        logger.info('in generateProprietaryNames')
        try:
            db = Database()
            conn = db.getPostgresConn()

            localDf1 = dataFrame.filter(dataFrame.tag == 'Drug')

            #filtering rows containing drug
            dataFrameWithOutDrug = dataFrame.filter(dataFrame.tag != 'Drug')
            
            def getMatcingDoc(drug_name,Item_description):
                if drug_name == '':
                    return '||||||'

                if "'" in drug_name:
                    drug_name = drug_name.replace("'","''")

                db = Database()
                connection = db.getDbConn()
                  
                cursor = connection.cursor(cursor_factory=NamedTupleCursor)
                postgreSQL_select_Query = "select proprietaryname,dosageformname,packagedescription,productndc,nonproprietaryname,labelername\
                     from product_package where lower(proprietaryname) like '%{}%' or lower(nonproprietaryname) like '%{}%' "\
                    .format(drug_name,drug_name)

                cursor.execute(postgreSQL_select_Query)

                records = cursor.fetchall()

                resultset = pandas.DataFrame(records)
                cursor.close()
   
                if len(resultset) == 0:
                    cursor = connection.cursor()

                    postgreSQL_select_Query = "select distinct(proprietaryname,1,2,ndc,nonproprietaryname,labelername) FROM animal_product where   lower(proprietaryname) like '%{}%' limit 1"\
                    .format(drug_name)

                    cursor.execute(postgreSQL_select_Query)
                    bestMatch = cursor.fetchone()

                    if bestMatch is None:
                        postgreSQL_select_Query = "select proprietaryname,1,2,ndc,nonproprietaryname,labelername FROM animal_product where   lower(nonproprietaryname) like '%{}%' limit 1"\
                        .format(drug_name)

                        cursor.execute(postgreSQL_select_Query)
                        bestMatch = cursor.fetchone()

                        if bestMatch is None:
                            cursor.close()
                            connection.close()
                            return '||||||'
                    bestMatch = '|'.join(str(i) for i in bestMatch)
                    cursor.close()
                    connection.close()
                    return  bestMatch+'|'       

                resultset = resultset.astype(str)
                matcingDocs = ["|".join(row) for _, row in resultset.iterrows()]
                bestMatch = process.extractOne(Item_description, matcingDocs,scorer=fuzz.token_sort_ratio)
                connection.close()
                return bestMatch[0]+'|'+str(bestMatch[1])

            udf = Functions.UserDefinedFunction(getMatcingDoc, StringType())

            #Identifying human and animal drug
            localDf = localDf1.groupBy(Functions.col("Drug_Name").alias('Drug_Name2'),Functions.col("Item_Description").alias('Item_Description2')).agg(Functions.lit('1'))

            localDf = localDf.withColumn("matched_doc",udf(Functions.col("Drug_Name2"),Functions.col("Item_Description2"))).drop('1')

            split_col = Functions.split(localDf['matched_doc'],'\|')
            localDf = localDf.withColumn("Proprietary_Name", split_col.getItem(0))\
                        .withColumn("Non-proprietary_Name", split_col.getItem(4)) \
                        .withColumn("NDC", split_col.getItem(3))\
                        .withColumn("NDC Labeler Name", split_col.getItem(5))\
                        .withColumn("NDC Accuracy", split_col.getItem(6)).drop('matched_doc')
            localDf = localDf1.join(localDf,(localDf.Drug_Name2==localDf1.Drug_Name) & (localDf.Item_Description2==localDf1.Item_Description)).drop(localDf.Item_Description2).drop(localDf.Drug_Name2)

            dataFrameWithOutDrug = dataFrameWithOutDrug.withColumn("Proprietary_Name",Functions.lit(''))\
                .withColumn("Non-proprietary_Name",Functions.lit('')).withColumn("NDC",Functions.lit(''))\
                    .withColumn("NDC Labeler Name",Functions.lit('')).withColumn("NDC Accuracy",Functions.lit(''))

    
            localDf =  dataFrameWithOutDrug.union(localDf)     
            
            data_abs_path=os.path.abspath(Constants.OUTPUT_LOCATION+'/'+project_name)

            if not os.path.exists(data_abs_path):
                os.makedirs(data_abs_path)
            
            localDf.persist()
            localDf.toPandas().to_csv('{}/cleaned.csv'.format(data_abs_path),index=False)
            logger.info('saved cleaned.csv in {} '.format(data_abs_path))
            return localDf
        
        except Exception as e:
            logger.exception('exception generateProprietaryNames {}'.format(e))
            sys.exit(1)
