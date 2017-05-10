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


def get_type_loc(line):
    if line[6] == 'KY_CD':
        return (1, 1), 0
    elif int(line[6]) in [341, 578, 344, 351, 109, 235, 361, 105, 107]:
        return (line[23], line[6]), 1
    else:
        return (1, 1), 0




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Input file error")
        exit(-1)
    sc = SparkContext()

    # line = sc.textFile(sys.argv[1], 1)\
    line = sc.textFile("NYPD_Complaint_Data_Historic.csv", 1)\
            .mapPartitions(lambda x: reader(x))\
            .map(get_type_loc)\
            .reduceByKey(lambda a, b: a + b)\
            .sortByKey()

    result = line.map(lambda (k, v): "{0},{1},{2}".format(k[0], k[1], v))\
                .saveAsTextFile("kycd_loc.csv")

    sc.stop()
