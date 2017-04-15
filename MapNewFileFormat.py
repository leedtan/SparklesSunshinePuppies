import sys
from operator import add
from pyspark import SparkContext
#import pyspark_csv as pycsv
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

def is_date_outside_range(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/((200[0-5])|(19\d\d)|(2016))', string):
        return True
    else:
        return False

def is_date_ridiculous(string):
    if re.findall('\s*(([0]\d)|([1][0-2]))/([0-2]\d|[3][0-1])/\d\d\d\d', string):
        return True
    else:
        return False


def is_time(string):
    if re.findall('(([01]\d)|(2[0-3]))\:([0-5]\d)\:([0-5]\d)', string):
        return True
    else:
        return False


def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Input file error")
        exit(-1)
    sc = SparkContext()
    line = sc.textFile(sys.argv[1], 1)\
            .mapPartitions(lambda x: reader(x))\
            .map(lambda x: ("_-^-^-^-_".join(x)))\
            .sortByKey(lambda x: x[0])

    result = line.map(lambda(k, v): "{0},{1}".format(k, v))\
                .saveAsTextFile("NYPD_PYSPARK2.csv")

    sc.stop()
