# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 19:14:08 2015

@author: zacha_000
"""

'''
Basic approach: 
    1. Restrict set of entries to crimes since 2011 (due to changing district boundaries)
    2. Find the number of crimes per beat per month, restrict set of entries to only beats that have had a crime in every month 2011-end
        2a. Make an ordered list of all such beats (255)
    3. Construct a set of vectors such that each vector is a month, and its entries are the number of crimes in each beat during that month
        ordered by beat (same beats as 2a)
    4. Find correlation matrix of this VectorRDD (255x255)
    5. Find the indices of the highest value of the correlation matrix (that isn't 1.0)
    6. Check 2a to see what beats match those indices
    7. Check http://gis.chicagopolice.org/pdfs/district_beat.pdf to see if those beats are neighbors
        - Repeat 5-7 until 7 is true (happened first time, sanity check)
        
Results:
    Beats 0825 and 0831 are the most highly correlated (.903) at the monthly level since 2011, and they border each other.
    Speculative explanation: These two beats are both in South-Central Chicago, near Midway airport. They are right on the border of the
    Southern region, which is notorious for high-crime, and not in the relative safety of downtown or the North side.
    
    I speculate that, when crime rates rise, areas like South-Central are the first to get hit, and beats in that area experience crime
    rises and falls in unison. More analysis (really, not that much more - just check correlations between these beats, should be a 
    block of relatively high values sort of in the center of the matrix) could confirm this. I'll see if I get time to do it.
'''


from datetime import datetime
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
import numpy as np

sc = SparkContext()

lines = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')
header = lines.first()
countfile = lines.filter(lambda x: x != header)
count2 = countfile.map(lambda x: x.split(','))
count3 = count2.map(lambda x: (x[10], x[2]))#only take beat and datetime

#want to take the unique month of the datetime - maybe just unique month and year

#make set (beat, month, year)
count4 = count3.map(lambda x: (x[0],datetime.strptime(x[1], '%m/%d/%Y %I:%M:%S %p').month,datetime.strptime(x[1], '%m/%d/%Y %I:%M:%S %p').year))
#count5 = count4.map(lambda x: (str(x[0])+'-'+str(x[1])+'-'+str(x[2]),1))
count4new = count4.filter(lambda x: x[2]>2010)#restrict since 2011
count4newer = count4new.filter(lambda x: len(x[0]) == 4)#get rid of beats that aren't ####
count5 = count4newer.map(lambda x: (str(x[0])+'-'+str(x[2])+'-'+str(x[1]),1))

# set = ('beat-year-month',1)
count6 = count5.reduceByKey(lambda a,b: a+b)#for each beat month, find # crime events reported

#now: count6 = ('beat-month-year',#number of crimes on that beat during that month)
count7 = count6.sortByKey()#order by beat, then year, then month (month not chronological, okay since it's same for each beat)

'''
old stuff:
#count7 = count6.map(lambda x: (x[0][0:4],x[1]))

#[('2001-1- PUBLIC', 2), ('2001-1-0111',150), ('2001-1-0112', 109),...]

#problem: public isn't dated; solution: delete it (only case, first, header)
#todelete = count7.first()
#count8 = count7.filter(lambda x: x!=todelete)
'''

count9 = count7.map(lambda x: (x[0][0:4],x[1]))#take just the beat count
count10 = count9.groupByKey()#group by beat
#problem: some beats don't have any crimes in a given month
    #solution: don't consider those beats that have a month without a crime reported
    #average # crimes per beat per month > 100, so a given beat that doesn't have a crime for a month isn't going to be of use.
count11 = count10.sortByKey()
count12 = count11.filter(lambda x: x[0]!= ' PRI' and x[0]!= ' PUB')#not needed anymore with count4newer
count13 = count12.filter(lambda x: len(x[1])==53)#256 beats have had a crime reported every month since 2011 (out of 283 total)
#only taking beats that have a crime event reported every month (53 months in total)

counter = count13.map(lambda x: x[0])
a = counter.take(256)#last value is 'true', address below
del a[255]# a is now an ordered list of all the beats for which there was a crime reported in every month since 2011

#now start working on aggregating across months instead of across beats (Statistics.corr works on columns, not rows)
counter8 = count7.filter(lambda x: x[0][0:4] in a)#check that we're only considering the 255 beats for which a crime is recorded in each considered month
counter9 = counter8.map(lambda x: (x[0][5:len(x[0])],x[1])) #make set ('year-month',count of crimes)#still at beat level
counter10 = counter9.groupByKey()#beats remain ordered as a
counter11 = counter10.map(lambda x: (x[0], list(x[1])))#expand iterable to list
counter12 = counter11.map(lambda x: Vectors.dense(x[1]))#VectorRDD of crime events by beat

b = Statistics.corr(counter12)#b is correlation matrix

#find the highest element of b that is not 1, as well as its row and column (beats in a that are correlated)
curmax = 0.0
indexOne = -1
indexTwo = -1
for i in np.arange(len(b)):
    for j in np.arange(len(b[i])):
        if b[i][j] > curmax and b[i][j] != 1:
            curmax = b[i][j]
            indexOne = i
            indexTwo = j

curmax#0.90305
indexOne#90
indexTwo#91

a = counter.take(256)#a is ordered array of considered beats, plus 'true' at the end
a[90] #'0825'
a[91] #'0831'
#according to http://gis.chicagopolice.org/pdfs/district_beat.pdf 0825 and 0831 are bordering
# 0825 and 0831 are the two beats which are most correlated at the monthly crime level since 2011, corr = 0.90305
