# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 06:58:16 2015

@author: zacha_000
"""
import numpy as np
from pyspark import SparkContext
from datetime import datetime
sc = SparkContext()
from matplotlib import pyplot as plt
from pyspark.sql import SQLContext, Row

sqlContext = SQLContext(sc)
filer = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')
file1 = filer.map(lambda x: x.split(','))
header = file1.first()
filer1 = file1.filter(lambda x: x!=header)#strip first row, headers
file2 = filer1.map(lambda x: Row(id=x[0],casenum = x[1], crimedate = datetime.strptime(x[2],'%m/%d/%Y %I:%M:%S %p')))

#create new rdd for each crime month
jancrime = file2.filter(lambda x: x.crimedate.month == 1)
febcrime = file2.filter(lambda x: x.crimedate.month == 2)
marcrime = file2.filter(lambda x: x.crimedate.month == 3)
aprcrime = file2.filter(lambda x: x.crimedate.month == 4)
maycrime = file2.filter(lambda x: x.crimedate.month == 5)
juncrime = file2.filter(lambda x: x.crimedate.month == 6)
julcrime = file2.filter(lambda x: x.crimedate.month == 7)
augcrime = file2.filter(lambda x: x.crimedate.month == 8)
sepcrime = file2.filter(lambda x: x.crimedate.month == 9)
octcrime = file2.filter(lambda x: x.crimedate.month == 10)
novcrime = file2.filter(lambda x: x.crimedate.month == 11)
deccrime = file2.filter(lambda x: x.crimedate.month == 12)

#it would be easier here to just get the counts of each monthcrime, but the assignment says to use SparkSQL
schema_jan = sqlContext.createDataFrame(jancrime)
schema_feb = sqlContext.createDataFrame(febcrime)
schema_mar = sqlContext.createDataFrame(marcrime)
schema_apr = sqlContext.createDataFrame(aprcrime)
schema_may = sqlContext.createDataFrame(maycrime)
schema_jun = sqlContext.createDataFrame(juncrime)
schema_jul = sqlContext.createDataFrame(julcrime)
schema_aug = sqlContext.createDataFrame(augcrime)
schema_sep = sqlContext.createDataFrame(sepcrime)
schema_oct = sqlContext.createDataFrame(octcrime)
schema_nov = sqlContext.createDataFrame(novcrime)
schema_dec = sqlContext.createDataFrame(deccrime)

#create tables
schema_jan.registerTempTable("jantable")
schema_feb.registerTempTable("febtable")
schema_mar.registerTempTable("martable")
schema_apr.registerTempTable("aprtable")
schema_may.registerTempTable("maytable")
schema_jun.registerTempTable("juntable")
schema_jul.registerTempTable("jultable")
schema_aug.registerTempTable("augtable")
schema_sep.registerTempTable("septable")
schema_oct.registerTempTable("octtable")
schema_nov.registerTempTable("novtable")
schema_dec.registerTempTable("dectable")

#get count of each table
jancount = sqlContext.sql("SELECT COUNT(*) FROM jantable")
febcount = sqlContext.sql("SELECT COUNT(*) FROM febtable")
marcount = sqlContext.sql("SELECT COUNT(*) FROM martable")
aprcount = sqlContext.sql("SELECT COUNT(*) FROM aprtable")
maycount = sqlContext.sql("SELECT COUNT(*) FROM maytable")
juncount = sqlContext.sql("SELECT COUNT(*) FROM juntable")
julcount = sqlContext.sql("SELECT COUNT(*) FROM jultable")
augcount = sqlContext.sql("SELECT COUNT(*) FROM augtable")
sepcount = sqlContext.sql("SELECT COUNT(*) FROM septable")
octcount = sqlContext.sql("SELECT COUNT(*) FROM octtable")
novcount = sqlContext.sql("SELECT COUNT(*) FROM novtable")
deccount = sqlContext.sql("SELECT COUNT(*) FROM dectable")

#actual count ints
jantotal = jancount.first()[0]
febtotal = febcount.first()[0]
martotal = marcount.first()[0]
aprtotal = aprcount.first()[0]
maytotal = maycount.first()[0]
juntotal = juncount.first()[0]
jultotal = julcount.first()[0]
augtotal = augcount.first()[0]
septotal = sepcount.first()[0]
octtotal = octcount.first()[0]
novtotal = novcount.first()[0]
dectotal = deccount.first()[0]

#monthly average
janavg = jantotal/15
febavg = febtotal/15
maravg = martotal/15
apravg = aprtotal/15
mayavg = maytotal/15
junavg = juntotal/15
julavg = jultotal/15
augavg = augtotal/15
sepavg = septotal/15
octavg = octtotal/15
novavg = novtotal/15
decavg = dectotal/15
#15 januarys, 15 feb, 15 mar, 15 apr, 15 may, 14 jun, 14 jul, 14 aug, 14 sep, 14 oct, 14 nov, 14 dec

#plot monthly averages
fig = plt.figure()
monthavg = (janavg, febavg, maravg, apravg, mayavg, junavg, julavg, augavg, sepavg, octavg, novavg, decavg)

ind = np.arange(12)
width = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, monthavg, width, color='r')

ax.set_ylabel('Average crime count')
ax.set_xlabel('Month')
ax.set_title('Average count of crimes by month')
ax.set_xticks(ind+width/2)
ax.set_xticklabels( ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec') )

plt.show()