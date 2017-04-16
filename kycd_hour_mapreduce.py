'''
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --executor-memory 4g
'''
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import os
import StringIO
import datetime
import numbers
import re


def str2date(x):
    x = x.strip()
    return datetime.datetime.strptime(x, '%m/%d/%Y')

def str2time(x):
    x = x.strip()
    return datetime.datetime.strptime(x, '%H:%M:%S')

def is_date(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/(20(([0][6-9])|([1][0-5])))', string):
        return True
    else:
        return False

def is_time(string):
    if re.findall('(([01]\d)|(2[0-3]))\:([0-5]\d)\:([0-5]\d)', string):
        return True
    else:
        return False

def get_hour(string):
    string = string.strip()
    return datetime.datetime.strptime(string, '%H:%M:%S').hour

def map_by_hour(line):
    if is_time(line[2]) and is_time(line[4]) and is_date(line[1]) and is_date(line[3])\
        and ( (str2date(line[1])<str2date(line[3])) 
            or (str2date(line[1])==str2date(line[3]) and str2time(line[2])<=str2time(line[4])) ):
        return (get_hour(line[2]), line[6]) , 1
    else:
        return (1, 1) , 0

################
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Input file error")
        exit(-1)
    sc = SparkContext()
    # line = sc.textFile("NYPD_Complaint_Data_Historic.csv", 1)\
    line = sc.textFile(sys.argv[1], 1)\
            .mapPartitions(lambda x: reader(x))\
            .map(map_by_hour)\
            .reduceByKey(lambda a, b: a + b)\
            .sortByKey()

    result = line.map(lambda (k, v): "{0},{1},{2}".format(k[0], k[1], v))\
                .saveAsTextFile("hour_valid.csv")

    sc.stop()
