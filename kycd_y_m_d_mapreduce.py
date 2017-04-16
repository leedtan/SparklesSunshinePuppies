#!/usr/bin/env python
'''
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --executor-memory 4g
'''

from pyspark import SparkContext
import os
from datetime import datetime
from datetime import date
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import re
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import IntegerType


def get_year(inputs):
    y, m, d = str(inputs).split('-')
    return int(y)

def get_month(inputs):
    y, m, d = str(inputs).split('-')
    return int(m)

def get_day(inputs):
    y, m, d = str(inputs).split('-')
    day = date(int(y), int(m), int(d)).isoweekday()
    return day

##############################################################################

def is_date_lv(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/(\d\d\d\d)', string):
        return True
    else:
        return False


def is_time_lv(string):
    if re.findall('(([01]\d)|(2[0-3]))\:([0-5]\d)\:([0-5]\d)', string):
        return True
    else:
        return False


to_date_lv = udf(lambda x: datetime.strptime(x.strip(), '%m/%d/%Y') if is_date_lv(x) else datetime.strptime('01/01/1001', '%m/%d/%Y'), DateType())
# to_time_lv = udf(lambda x: datetime.strptime(x.strip(), '%H:%M:%S') if is_time_lv(x) else datetime.strptime('01:01:01', '%H:%M:%S'), DateType())

to_year = udf(lambda x: get_year(x), IntegerType())
to_month = udf(lambda x: get_month(x), IntegerType())
to_day = udf(lambda x: get_day(x), IntegerType())

# read the crime data into a dataframe
df = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema='true', sep=', ').load('NYPD_Complaint_Data_Historic.csv')
df = df.withColumn('CMPLNT_TO_DT', to_date_lv(col('CMPLNT_TO_DT')))
df = df.withColumn('CMPLNT_FR_DT', to_date_lv(col('CMPLNT_FR_DT')))
# df = df.withColumn('CMPLNT_TO_TM', to_time_lv(col('CMPLNT_TO_TM')))
# df = df.withColumn('CMPLNT_FR_TM', to_time_lv(col('CMPLNT_FR_TM')))
df = df.withColumn('RPT_DT', to_date_lv(col('RPT_DT')))

# read the crime data types into the dataframe
typedf = sqlContext.read.format('com.databricks.spark.csv').options(header = False, inferschema='true').load('types.csv')

header = ['id','CMPLNT_NUM','CMPLNT_FR_DT','CMPLNT_FR_TM','CMPLNT_TO_DT',
        'CMPLNT_TO_TM','RPT_DT','KY_CD','OFNS_DESC','PD_CD','PD_DESC',
        'CRM_ATPT_CPTD_CD','LAW_CAT_CD','JURIS_DESC','BORO_NM',
        'ADDR_PCT_CD','LOC_OF_OCCUR_DESC','PREM_TYP_DESC','PARKS_NM',
        'HADEVELOPT','X_COORD_CD','Y_COORD_CD','Latitude','Longitude','Lat_Lon']

for i in range(25):
   typedf = typedf.withColumnRenamed("C" + str(i), header[i])


# Create sql tables
# sqlContext.registerDataFrameAsTable(df,'crimedata')
sqlContext.registerDataFrameAsTable(typedf,'crimetype')

##############################################################################

# Create new sql dateframe
df_temp = df.withColumn('CMPLNT_YEAR', to_year(col('CMPLNT_FR_DT')))
df_temp = df_temp.withColumn('CMPLNT_MONTH', to_month(col('CMPLNT_FR_DT')))
df_dt = df_temp.withColumn('CMPLNT_DAY', to_day(col('CMPLNT_FR_DT')))

# Create new sql table
sqlContext.registerDataFrameAsTable(df_dt,'crimedt')


###############################
# Time Series Analysis - Year #
###############################
df_year_valid = sqlContext.sql("\
                SELECT crimedt.CMPLNT_YEAR, crimedt.KY_CD, count(*) as c \
                FROM crimetype \
                INNER JOIN crimedt \
                ON crimedt.CMPLNT_NUM = crimetype.id \
                WHERE crimetype.CMPLNT_FR_DT = 'date' \
                      AND crimetype.CMPLNT_TO_DT = 'date' \
                      AND crimedt.CMPLNT_TO_DT >= crimedt.CMPLNT_FR_DT\
                GROUP BY crimedt.CMPLNT_YEAR, crimedt.KY_CD\
                ORDER BY crimedt.CMPLNT_YEAR, c DESC\
                      ")

lines = df_year_valid.map(lambda x: (x[0], ",".join([ str(z) for z in x[1:] ])))

with open("year_valid.csv", "a") as out_file:
    for line in lines.collect():
        result = re.findall('\d+', str(line))
        print >>out_file, ','.join(result)


################################
# Time Series Analysis - Month #
################################
df_month_valid = sqlContext.sql("\
                SELECT crimedt.CMPLNT_MONTH, crimedt.KY_CD, count(*) as c \
                FROM crimetype \
                INNER JOIN crimedt \
                ON crimedt.CMPLNT_NUM = crimetype.id \
                WHERE crimetype.CMPLNT_FR_DT = 'date' \
                      AND crimetype.CMPLNT_TO_DT = 'date' \
                      AND crimedt.CMPLNT_TO_DT >= crimedt.CMPLNT_FR_DT\
                GROUP BY crimedt.CMPLNT_MONTH, crimedt.KY_CD\
                ORDER BY crimedt.CMPLNT_MONTH, c DESC\
                      ")

lines = df_month_valid.map(lambda x: (x[0], ",".join([ str(z) for z in x[1:] ])))

with open("month_valid.csv", "a") as out_file:
    for line in lines.collect():
        result = re.findall('\d+', str(line))
        print >>out_file, ','.join(result)



##############################
# Time Series Analysis - Day #
##############################
df_day_valid = sqlContext.sql("\
                SELECT crimedt.CMPLNT_DAY, crimedt.KY_CD, count(*) as c \
                FROM crimetype \
                INNER JOIN crimedt \
                ON crimedt.CMPLNT_NUM = crimetype.id \
                WHERE crimetype.CMPLNT_FR_DT = 'date' \
                      AND crimetype.CMPLNT_TO_DT = 'date' \
                      AND crimedt.CMPLNT_TO_DT >= crimedt.CMPLNT_FR_DT\
                GROUP BY crimedt.CMPLNT_DAY, crimedt.KY_CD\
                ORDER BY crimedt.CMPLNT_DAY, c DESC\
                      ")

lines = df_day_valid.map(lambda x: (x[0], ",".join([ str(z) for z in x[1:] ])))

with open("day_valid.csv", "a") as out_file:
    for line in lines.collect():
        result = re.findall('\d+', str(line))
        print >>out_file, ','.join(result)

