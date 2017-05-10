'''
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 --executor-memory 6g
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


def is_date(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/(20(([0][6-9])|([1][0-5])))', string):
        return True
    else:
        return False


def get_year(inputs):
    if is_date(inputs):
        m, d, y = str(inputs).split('/')
        return int(y)
    else:
        return 1


def get_yearly_boro_type(line):
    if line[6] == 'KY_CD':
        return (1, 1, 1), 0
    elif int(line[6]):
        return (get_year(line[1]), line[13], line[6]), 1
    else:
        return (1, 1, 1), 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Input file error")
        exit(-1)
    sc = SparkContext()

    # line = sc.textFile("NYPD_Complaint_Data_Historic.csv", 1)\
    line = sc.textFile(sys.argv[1], 1)\
            .mapPartitions(lambda x: reader(x))\
            .map(get_yearly_boro_type)\
            .reduceByKey(lambda a, b: a + b)\
            .sortByKey()

    result = line.map(lambda (k, v): "{0}\t{1}\t{2}\t{3}".format(k[0], k[1], k[2], v))\
                .saveAsTextFile("yearly_boro_ttl.csv")

    sc.stop()
