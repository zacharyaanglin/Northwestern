# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 12:05:38 2015

@author: zacha_000
"""

from datetime import datetime
from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np

sc = SparkContext()
	
# Load data
lines = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')

#Remove Header
header = lines.first()
countfile = lines.filter(lambda x: x != header)
count2 = countfile.map(lambda x: x.split(','))
count3 = count2.map(lambda x: datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p'))

hours = count3.map(lambda x: (x.hour,1))
weekdays = count3.map(lambda x: (x.weekday(),1))
months = count3.map(lambda x: (x.month,1))

hourscount = hours.reduceByKey(lambda a,b: a+b)
weekdayscount = weekdays.reduceByKey(lambda a,b: a+b)
monthscount = months.reduceByKey(lambda a,b: a+b)

hourscount2 = hourscount.sortByKey()
weekdayscount2 = weekdayscount.sortByKey()
monthscount2 = monthscount.sortByKey()

hoursdata = hourscount2.collect()
weekdaysdata = weekdayscount2.collect()
monthsdata = monthscount2.collect()

#Hours
'''
Dumb approach, see below

midnight = 326416
one = 185828
two = 155351
three = 124522
four = 92972
five = 76285
six = 89800
seven = 130299
eight = 194829
nine = 245697
ten = 241402
eleven = 255168
noon = 327281
onepm = 275314
twopm = 293871
threepm = 307399
fourpm = 288346
fivepm = 293000
sixpm = 315714
'''

hourstuff = [(0, 326416), (1, 185828), (2, 155351), (3, 124522), (4, 92972), (5, 76285), (6, 89800), (7, 130299), (8, 194829), (9, 245697), (10, 241402), (11, 255168), (12, 327281), (13, 275314), (14, 293871), (15, 307399), (16, 288346), (17, 293000), (18, 315714), (19, 330763), (20, 333495), (21, 327930), (22, 323516), (23, 266646)]
hourcounts= []
for i in np.arange(len(hourstuff)):
    hourcounts.append(hourstuff[i][1])
    
daystuff = [(0, 817722), (1, 834011), (2, 839315), (3, 828943), (4, 874708), (5, 827701), (6, 779444)]
daycounts = []
for i in np.arange(len(daystuff)):
    daycounts.append(daystuff[i][1])
    
monthstuff = [(1, 463497), (2, 407957), (3, 492905), (4, 494231), (5, 521509), (6, 504706), (7, 531295), (8, 524592), (9, 492694), (10, 500018), (11, 448726), (12, 419714)]
monthcounts = []
for i in np.arange(len(monthstuff)):
    monthcounts.append(monthstuff[i][1])
 

ind = np.arange(12)
width = 0.35   
fig, ax = plt.subplots()
#rects1 = ax.bar(ind, hourcounts, width, color = 'r')
#rects2 = ax.bar(ind, daycounts, width, color = 'g')
rects3 = ax.bar(ind, monthcounts, width, color = 'b')
ax.set_ylabel('Total crime count')
ax.set_xlabel('Month')
ax.set_title('Total crime numbers by month')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec') )

plt.show()