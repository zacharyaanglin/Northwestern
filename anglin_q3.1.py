# -*- coding: utf-8 -*-
"""
Created on Tue Jun 09 15:43:25 2015

@author: zacha_000
"""
'''
Task:

Predict the number of crime events in the next week at the beat level. The higher the IUCR is,
the more severe the crime is. Violent crime events are more important and thus it is desirable
that they are forecasted more accurately. (45 pts) You are encouraged to bring in additional
data sets. (extra 50 pts if you mix the existing data with an exogenous data set) Report the
accuracy of your models.

Approach: 

Obtain crime rates from previous weeks at the beat level, along with the top few IUCRs?
The lower the IUCR, the more severe the crime is. 
Isolate just the numeric portion of the IUCR, find the average IUCR at each beat for each week along with number of crimes.
'''
from pyspark.mllib.regression import LinearRegressionWithSGD
from datetime import datetime, date
import numpy as np
import pandas as pd
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.linalg import Vectors

from pyspark import SparkContext
sc = SparkContext()

#Are you building a model to strictly predict violent crimes?
violent = False

lines = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/crime')
header = lines.first()
countfile = lines.filter(lambda x: x != header)

beat = 10
dater = 2
iucr = 4
beats = countfile.map(lambda x: x.split(','))
beats2 = beats.map(lambda x: (x[dater], x[beat], x[iucr]))#take just dates, beats, iucrs
beats = beats2.filter(lambda x: len(x[1]) == 4 and x[1]!='true')#only take numeric beats, not PUBLIC, PRIVATE, TRUE, etc

#below, only take the violent crimes for construction of a violence-specific model
if(violent):#ONLY TAKE IUCR CODES INDICATED AS SPECIFICALLY VIOLENT BY ILLINOIS POLICE
    beats = beats.filter(lambda x: x[2][0:2]=='01' or x[2][0:2]=='02' or x[2][0:2]=='03' or x[2][0:2]=='04' or x[2][0:2]=='05')
#figure out how to compile datetimes down to weeks.

#change all of these to Violent for the violent model
#get time since Jan 7, 2001 (even number weeks until 5-31-2015)
beats2 = beats.map(lambda x: (datetime.strptime(x[0],'%m/%d/%Y %I:%M:%S %p') - datetime(2014,4,30), x[1], x[2]))
beats3 = beats2.map(lambda x: (x[0].days, x[1],x[2]))#get number of days since April 30, 2014
beats3new = beats3.filter(lambda x: x[0]>=0)
beats4 = beats3new.map(lambda x: (x[0], str(x[1])+'-'+str(x[2])))

weather = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser58/Spark/CustomHistory.csv')
weather1 = weather.map(lambda x: x.split(','))
weather2 = weather1.filter(lambda x: len(x[0]) <=10 and x[0]!='')#gives weather data
weather3 = weather2.map(lambda x: (x[0],x[1],x[2],x[3]))#date, max temp, mean temp, min temp
weather4 = weather3.map(lambda x: (datetime.strptime(x[0],'%Y-%m-%d') - datetime(2014,4,30), x[1], x[2], x[3]))
weather5 = weather4.map(lambda x: (x[0].days,x[1],x[2],x[3]))# (# days since april 30 2014, max temp, mean temp, min temp)
weather6 = weather5.map(lambda x: (x[0],str(x[1])+'-'+str(x[2])+'-'+str(x[3])))#(numdays, max-mean-min)

#have both beats and weather date in key-value form, join on number of days since April 30
mix = beats4.join(weather6)
#success: mix.first() = (0, ('0423-2025', '53-48-43')) --outside data set is incorporated
#(numdays since April 30, 2014, ('beat-iucr','maxtemp-meantemp-mintemp'))

mix1 = mix.map(lambda x: (x[0], x[1][0].split('-'), x[1][1].split('-')))
mix2 = mix1.map(lambda x: (x[0], x[1][0], x[1][1], x[2][0], x[2][1], x[2][2]))
# num days since april 30, beat, iucr, max temp, mean temp, min temp
# ex: (0, '0423', '2025', '53', '48', '43')
mix3 = mix2.map(lambda x: (str(x[0])+'-'+str(x[1]), (x[2], x[3], x[4],x[5])))
mix4 = mix3.groupByKey()
mix5 = mix4.map(lambda x: (x[0],list(x[1])))
# (days-beat, [(iucr1,max, mean, min),(iucr2,max,mean,min),...])

mix6 = mix5.map(lambda x: (x[0],x[1][0][2],len(x[1])))
#mix6: ('days-beat','mean temp that day', number of crimes that day)
#ex: ('1-0631','46',9) <- clearly gives the number of crimes at each beat on each day and the temperature that day

mix7 = mix6.map(lambda x: (x[0].split('-'),x[1],x[2]))
mix8 = mix7.map(lambda x: (x[0][0],x[0][1],x[1],x[2]))
#('day','beat','temp',numcrimes)
mixer9 = mix8.map(lambda x: (int(x[0])/7,x[1],x[2],x[3]))
mixer9new = mixer9.filter(lambda x: x[2]!='')
#(week of day, beat, mean temp, number of crimes that day)
mixer10 = mixer9new.map(lambda x: (str(x[0])+'-'+str(x[1]), (int(x[2]), x[3])))
#('week-beat', ('mean temp', number of crimes))
mixer11 = mixer10.groupByKey()
mixer12 = mixer11.map(lambda x: (x[0],list(x[1])))
mixer13 = mixer12.map(lambda x: (x[0], pd.DataFrame(x[1]).mean()[0], pd.DataFrame(x[1]).sum()[1]))
#('week-beat', average temperature for days on which crimes were reported that week, total number of crimes reported that week)

mixer14 = mixer13.map(lambda x: (x[0].split('-'),x[1],x[2]))
mixer15 = mixer14.map(lambda x: (int(x[0][0]),x[0][1],x[1],x[2]))
mixer16 = mixer15.map(lambda x: (x[0],(x[1],x[2],x[3])))
mixer17 = mixer16.sortByKey()
mixer18 = mixer17.map(lambda x: (x[1][0], x[0], x[1][1], x[1][2]))
mixer19 = mixer18.map(lambda x: (x[0], (x[1],x[2],x[3])))#cut out week number?
mixer20 = mixer19.groupByKey()#count = 274
mixer21 = mixer20.filter(lambda x: len(x[1]) == 55)#count = 267
mixer22 = mixer21.map(lambda x: (x[0], list(x[1])))
mixer23 = mixer22.sortByKey()


#('beat', [(week0, mean temp, num crimes),(week1,meantemp,numcrimes),...])
lin19 = mixer18.map(lambda x: (x[0], (x[3],x[2])))
lin20 = lin19.groupByKey()
lin22 = lin20.map(lambda x: (x[0], list(x[1])))
lin23 = lin22.sortByKey()#274 beats
'''
THIS IS WHERE YOU NEED TO SELECT THE BEAT YOU ARE TRYING TO PREDICT
'''
linReg1 = lin23.filter(lambda x: x[0]=='1011')#INPUT BEAT YOU WANT TO PREDICT HERE
lin24 = linReg1.map(lambda x: x[1])#only take vector of points
temp = lin24.first()#only one vector, because you've filtered by beat, but want to access it directly
for i in  np.arange(len(lin24.first())):
    temp[i] = LabeledPoint(temp[i][0],[temp[i][1]])
temp1 = sc.parallelize(temp)
features = temp1.map(lambda x: x.features)
scaler1 = StandardScaler().fit(features)
scaler2 = StandardScaler(withMean = True, withStd = True).fit(features)
label = temp1.map(lambda x: x.label)
data1 = label.zip(scaler1.transform(features))
data2 = label.zip(scaler1.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
data3 = data1.map(lambda x: LabeledPoint(x[0],x[1]))

####MODELING####
model = LinearRegressionWithSGD.train(data3)
valuesAndPreds = data3.map(lambda p: (p.label, model.predict(p.features)))
for i in valuesAndPreds.collect():
    print str(i)
MSE = valuesAndPreds.map(lambda (v,p): (v-p)**2).reduce(lambda x, y: x+y)/valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))#MSE for first beat (1011) = 139.585836153
#MSE for first beat (1011) (violent) = 17.4723869121

#Prediction for violent crimes next week in this beat: 4.14
################ Cutoff


################ Cutoff


#('beat','(#crimes week 1, # crimes week 2, # crimes week 3, ...))
'''
Now, take data set, turn it into a Vector if we can do time-series prediction for the next week. Should be mostly seasonality.
Hopefully, I can just input a vector and have it tell me the prediction for the next number based on seasonality

Also, find external dataset to incorporate, mostly through datetime feature, merge based off datetime.
-- if I pull that off, I don't even have to do the last problem.
'''