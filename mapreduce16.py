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

def str2date(x):
    x = x.strip()
    try:
        x = datetime.datetime.strptime(x, '%m/%d/%Y')
    except ValueError:
        return False

def str2time(x):
    x = x.strip()
    try:
        x = datetime.datetime.strptime(x, '%H:%M:%S')
    except ValueError:
        return False

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

idx = 16
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Input file error")
        exit(-1)
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)
    line = data.filter(lambda l: not l.startswith('CMPLNT'))\
            .mapPartitions(lambda x: reader(x))\
            .map(lambda x: (
	    x[0],   # key
            [x[idx],   # v[0]
	    'Empty' if x[idx].strip() == '' else
            'NULL' if x[idx].lower() == 'null' else
            'date' if is_date(x[idx]) else
            'OutDate' if is_date_outside_range(x[idx]) else
            'WayODate' if is_date_ridiculous(x[idx]) else
            'time' if is_time(x[idx]) else
            '24hTime' if is_24(x[idx]) else
            ('int' if abs(int(float(x[idx])) - float(x[idx])) < .0000001 else 'float')
				if isfloat(x[idx]) else
            'tuple' if (re.findall('\(.*[,].*\)', x[idx]) != []
			and x[idx].strip()[0] == '(' and x[idx].strip()[-1] == ')') else
            type(x[idx]).__name__  # v[1]
	    ]))\
            .sortByKey(lambda x: x[0])

    result = line.map(lambda(k, v): "{0},{1},{2},{3}".format(
		v[0] if v[1] != 'Empty' else 'RECORD EMPTY',
	    'Empty' if v[1] == 'Empty' else 'TEXT',
		'Premise Type Description',
		'INVALID' if v[1] == 'Empty' else 'VALID')
		)\
                .saveAsTextFile("PREM_TYP_DESC.out")

    sc.stop()
