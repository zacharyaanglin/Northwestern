# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 14:20:26 2015

@author: zacha_000
"""

'''
Task: Determine if crime is worse under Emanuel or Daley at granularity of choice, not just city level.
Approach: Classify crime events as occurring under Emanuel or Daley (Split May 16, 2011)
            Count the number of crime events under each mayor
            Determine the number of days in data set under each mayor
            Determine average daily crime rate for direct comparison
            
            For granularity:
                Find number of homicide crimes (drill down to IUCRs 0110, 0130, 0141, 0142) under each mayor
                Find average daily homicide rate
                Find proportion of crimes that are homicides
                
Results: Crime is generally better under Mayor Emanuel than it was under Mayor Daley.
            Average daily crime rate (number of crimes reported per day) is down, as is the homicide rate specifically.
            However, homicides are not as far down as the general crime rate. They make up a much larger proportion of total crime than they
            did under Daley.
'''
#compare mayor emmanuel to mayor daley
#daley leaves office May 16, 2011

from datetime import datetime, date
from pyspark import SparkContext
sc = SparkContext()

lines = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')
header = lines.first()
mayorfile = lines.filter(lambda x: x != header)
#mayorfile (all crime records) = 5801844 records

mayor2 = mayorfile.map(lambda x: x.split(','))
mayor3 = mayor2.map(lambda x: (x[2], x[3]))#only take datetime and block

#ignore inauguration day
emanuel = mayor3.filter(lambda x: datetime.strptime(x[0], '%m/%d/%Y %I:%M:%S %p') > datetime(2011,5,16))
daley = mayor3.filter(lambda x: datetime.strptime(x[0], '%m/%d/%Y %I:%M:%S %p') < datetime(2011,5,16))

###create a 1 for every emanuel, daley crime report
###emanuel_pairs = emanuel.map(lambda x: 1)
###daley_pairs = daley.map(lambda x: 1)

iucrs = mayor2.map(lambda x: (x[2],x[4]))#take datetime and iucr code
emanuel_iucr = iucrs.filter(lambda x: datetime.strptime(x[0], '%m/%d/%Y %I:%M:%S %p') > datetime(2011,5,16))
daley_iucr = iucrs.filter(lambda x: datetime.strptime(x[0], '%m/%d/%Y %I:%M:%S %p') < datetime(2011,5,16))

emanuel_pairs = emanuel_iucr.map(lambda x: (x[1],1))#get pairs of iucr code, 1 in emanuel tenure
daley_pairs = daley_iucr.map(lambda x: (x[1],1))#get pairs of iucr code, 1 in daley tenure
emanuel_codecounts = emanuel_pairs.reduceByKey(lambda a, b: a + b)#find counts of each iucr code
daley_codecounts = daley_pairs.reduceByKey(lambda a, b: a + b)#find counts of each iucr code

#take just the homicide counts
emanuel_homicidecounts = emanuel_codecounts.filter(lambda x: x[0] in '0110','0130','0141','0142')
daley_homicidecounts = daley_codecounts.filter(lambda x: x[0] in '0110','0130','0141','0142')

#reduce down to single tuple
emanuel_homicides = emanuel_homicidecounts.reduce(lambda a,b: a+b) #for some reason, not able to add counts directly, make tuple with all attributes
daley_homicides = daley_homicidecounts.reduce(lambda a,b: a+b)

#add up counts for each type of homicide
e_homicidecount = emanuel_homicides[1]+emanuel_homicides[3]+emanuel_homicides[5]#1803
d_homicidecount = daley_homicides[1]+daley_homicides[3]+daley_homicides[5]#5287

#get total counts of crimes
emanuel_count = emanuel.count()
daley_count = daley.count()

#find number of days' worth data for emanuel, daley
emanuel_days = (datetime(2015,5,31) - datetime(2011,5,16)).days
daley_days = (datetime(2011,5,16) - datetime(2001,1,1)).days

#compare average daily crime rate to account for differing timespans in date
emanuel_daily = emanuel_count/emanuel_days#836 average crimes per day
daley_daily = daley_count/daley_days#1206 average crimes per day

emanuel_dailymurders = float(e_homicidecount)/float(emanuel_days)#1.222 average murders per day
daley_dailymurders = float(d_homicidecount)/float(daley_days)#1.396 average murders per day

emanuel_murderpercentage = emanuel_dailymurders / emanuel_daily#0.00146, .15% of crimes
daley_murderpercentage = daley_dailymurders / daley_daily#0.00039, .04% of crimes (1/4 Emanuel's rate)

#alternatively:
#emanuel_murderpercentage = float(e_homicidecount)/float(emanuel_count)
#daley_murderpercentage = float(d_homicidecount)/float(daley_count)

#both total crimes and murders are down in emanuel's tenure from daley's tenure on a daily basis
#however, homicides make up a greater proportion of crimes in emanuel's tenure

