from framework.spark.setup import SparkSetup
from framework.spark.datareader import DataReader
from processors.dataCleaningProcessor import DataCleaner
from processors.duplicateIdentificationProcessor import DuplicateIdentifier
from processors.canonicalNameProcessor import CanonicalGenerator
from processors.drugnameProcessor import DrugNameGenerator
from processors.UnspscCodeProcessor import UnspscCodeGenerator
from processors.NdcProcessor import NdcGenerator
from processors.CtakesProcessor import RxNormGenerator
from processors.RxNavProcessor import RxInfoGenerator
from framework.spark.constants import Constants
import pathlib
import pandas
from pyspark.sql.types import *
import os
from processors.priorityDrugs import PriorityDrugGenerator
import sys
import datetime

import logging
LOG_FILENAME = 'logs.txt'
FORMAT = "%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,format=FORMAT)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.info('##############  Starting process ##############')

if len(sys.argv) < 6:
    logger.error('wrong number of command line arguments ---Exiting---!!! ')
    logger.info('example 1: python3 app.py 70 UNSPSC_CODE Project1 FULL FUZZY')                
    logger.info('example 2: python3 app.py 60 MANUFACTURER Project1 PARTIAL FAST_FUZZY')                
    logger.info('example 3: python3 app.py 50 NEW_SPEND_INFO Project2 FULL FUZZY')
    sys.exit(1)

Threshold = int(sys.argv[1])
Matching_param = str(sys.argv[2]).upper()
Project_name = str(sys.argv[3])
Process_type = str(sys.argv[4]).upper()
matching_type = str(sys.argv[5]).upper() 
ConsiderOnlyTopScore = True

if len(sys.argv) ==7:
    if str(sys.argv[6]).upper() =='FALSE':
        ConsiderOnlyTopScore = False

if matching_type == 'FUZZY':
    from processors.priceMatching_fuzzy import PriceInfoGenerator
elif matching_type == 'FAST_FUZZY':
    from processors.priceMatching import PriceInfoGenerator



if ((Matching_param != 'UNSPSC_CODE' and Matching_param != 'MANUFACTURER' and Matching_param != 'NEW_SPEND_INFO')\
    or (Process_type !='FULL' and Process_type !='PARTIAL' ) or (matching_type !='FUZZY' and matching_type !='FAST_FUZZY')):
    logger.error('Invalid Entries...! ---Exiting---!!! ')
    logger.info('example 1: python3 app.py 70 UNSPSC_CODE Project1 FULL FUZZY')                
    logger.info('example 2: python3 app.py 60 MANUFACTURER Project1 PARTIAL FAST_FUZZY')                
    logger.info('example 3: python3 app.py 50 NEW_SPEND_INFO Project2 FULL FUZZY')
    sys.exit(1)

try:
    setup = SparkSetup()
    reader = DataReader()
    cleaner = DataCleaner()
    dedup = DuplicateIdentifier()
    canon = CanonicalGenerator()
    drug = DrugNameGenerator()
    unscpc = UnspscCodeGenerator()
    ndc = NdcGenerator()
    #rxnorm = RxNormGenerator()
    #rxinfo = RxInfoGenerator()
    price = PriceInfoGenerator()
    prior = PriorityDrugGenerator()

    sparkContext, sparkSession = setup.setupSpark()

    dt1 = datetime.datetime.now()

    if Process_type == 'FULL':

        dataFrame = reader.ReadData(sparkSession)
        cleanedDataFrame = cleaner.cleanDataFrame(dataFrame = dataFrame)
        duplicatedDF = dedup.markDuplicate(dataFrame = cleanedDataFrame)
        newDF = canon.generateCanonicalName(dataFrame = duplicatedDF,filename='AliasMap.xlsx')
        newDF = drug.generateDrugNames(dataFrame = newDF,filename='O2 Keyword Mapping.xlsx')
        newDF = unscpc.generateUnspscCode(dataFrame = newDF ,sparkSession=sparkSession )
        newDF = ndc.generateProprietaryNames(newDF,Project_name)
        dt2 = datetime.datetime.now()
        logger.info('upto NDC Time - {}'.format(dt2-dt1))

    else:    

        src='../data/output/'+'/'+Project_name
        filename='cleaned.csv'
        pandasDataFrame = pandas.read_csv('{}/{}'.format(src,filename))
        logger.info('reading from cleaned.csv completed')
        pandasDataFrame.fillna('',inplace=True)
        schema =StructType([StructField('Key',StringType(),True),StructField('Item_Description',StringType(),True)\
                ,StructField('Item Number' ,StringType(),True),StructField('Manufacturer',StringType(),True)\
                ,StructField('Canonical_Manufacturer',StringType(),True),StructField('Drug_Name',StringType(),True)\
                ,StructField('tag',StringType(),True),StructField('Exculed',StringType(),True),StructField('PIM',IntegerType(),True)\
                ,StructField('Unspsc_Code',StringType(),True)
                ,StructField('Proprietary Name',StringType(),True),StructField('Non Proprietary Name',StringType(),True)\
                ,StructField('NDC',StringType(),True),StructField('Ndc Labeler Name',StringType(),True)\
                ,StructField('NDC Accuracy',StringType(),True)])
        newDF = sparkSession.createDataFrame(pandasDataFrame,schema=schema)
        pandasDataFrame = None

    newDF = prior.generatePriorityDrugsInfo(sparkSession,newDF,'priorityDrug.xlsx',Threshold,matching_type)
    price.generateSpendInfo(sparkContext,sparkSession,dataFrame = newDF,shouldConsiderOnlyTopScore=ConsiderOnlyTopScore,matchingThreshold=Threshold,matchingflag=Matching_param,projectName=Project_name)

    dt9 = datetime.datetime.now()

    logger.info('Complete Time - {}'.format(dt9-dt1))
    sparkContext.stop()

except Exception as e:
    logger.exception('exception occured {}'.format(e))
    sys.exit(1)