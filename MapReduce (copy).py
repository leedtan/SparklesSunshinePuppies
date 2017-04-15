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

def is_24(string):
    if re.findall('(24)\:([0-5]\d)\:([0-5]\d)', string):
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
            .map(lambda x: (x[0], ",".join(
            ['Empty' if z.strip() == '' else
            'NA' if (z.upper() == 'N/A' or z.upper() == 'UNSPECIFIED') else
            '999' if (z == '999'  or z == 999) else
            '99' if (z == '99' or z == 99) else
            'NaN' if z.lower() == 'nan' else
            'NULL' if z.lower() == 'null' else
            'Bool' if z.lower() in ['true', 'false'] else
            'date' if is_date(z) else
            'OutDate' if is_date_outside_range(z) else
            'WayODate' if is_date_ridiculous(z) else
            'time' if is_time(z) else
            '24hTime' if is_24(z) else
            ('int' if abs(int(float(z)) - float(z)) < .0000001 else 'float') if isfloat(z) else
            'tuple' if (re.findall('\(.*[,].*\)', z) != [] and z.strip()[0] == '(' and z.strip()[-1] == ')') else
            type(z).__name__
            for z in x[1]])))\
            .sortByKey(lambda x: x[0])

    result = line.map(lambda(k, v): "{0},{1}".format(k, v))\
                .saveAsTextFile("types.csv")

    sc.stop()
