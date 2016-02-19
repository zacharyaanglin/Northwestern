# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 12:22:16 2015

@author: zacha_000
"""

'''
Task: find top ten blocks for crimes in the last three years without using SparkSQL

Results: toptenblocks.txt
'''
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext()

lines = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')
header = lines.first()
countfile = lines.filter(lambda x: x != header)#get rid of first line, column names
#countfile (all crime records) = 5801844 records

count2 = countfile.map(lambda x: x.split(','))
count3 = count2.map(lambda x: (x[2], x[3]))#only take datetime and block

#filter down to days after May 31, 2012 (latest three years of data)
#it says now on the discussion board that we don't have to do this, but I already had when that was posted
count4 = count3.filter(lambda x: datetime.strptime(x[0], '%m/%d/%Y %I:%M:%S %p') > datetime(2012,5,31))
#count4 = 870732 records

count5 = count4.filter(lambda x: (x[1]))#take just the block, every crime event in last three years is now just a block name

pairs = count5.map(lambda x:(x,1))#create pairs, (blockname, 1)

counts = pairs.reduceByKey(lambda a, b: a + b)#count crimes for each block
counts2 = counts.map(lambda x: (x[1],x[0]))#switch order of key, value
sortedcount = counts2.sortByKey(False)#sort descending of key (really value, count of crimes)
sortedcount.take(10)#take the top ten blocks for crimes in the three year period