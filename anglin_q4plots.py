# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 12:54:39 2015

@author: zacha_000
"""
import numpy as np
import matplotlib.pyplot as plt

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
    
monthcounts[0] = monthcounts[0]/(15*31)
monthcounts[1] = monthcounts[1]/(28*12 + 19*3)
monthcounts[2] = monthcounts[2]/(15*31)
monthcounts[3] = monthcounts[3]/(15*30)
monthcounts[4] = monthcounts[4]/(15*31)
monthcounts[5] = monthcounts[5]/(14*30)
monthcounts[6] = monthcounts[6]/(14*31)
monthcounts[7] = monthcounts[7]/(14*31)
monthcounts[8] = monthcounts[8]/(14*30)
monthcounts[9] = monthcounts[9]/(14*31)
monthcounts[10] = monthcounts[10]/(14*30)
monthcounts[11] = monthcounts[11]/(14*31)



width = 0.35   
fig, ax = plt.subplots()

#can only chart one at a time (other two commented out)
'''
ind = np.arange(24)
ax.set_xticklabels( ('Midnight', '1AM', '2AM', '3AM', '4AM', '5AM', '6AM', '7AM', '8AM', '9AM', '10AM', '11AM', 'Noon', '1PM', '2PM', '3PM', '4PM', '5PM', '6PM', '7PM', '8PM', '9PM', '10PM', '11PM') )
rects1 = ax.bar(ind, hourcounts, width, color = 'r')
ax.set_ylabel('Total count of crimes')
ax.set_xlabel('Hour')
ax.set_title('Total count of crimes by hour')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Midnight', '1AM', '2AM', '3AM', '4AM', '5AM', '6AM', '7AM', '8AM', '9AM', '10AM', '11AM', 'Noon', '1PM', '2PM', '3PM', '4PM', '5PM', '6PM', '7PM', '8PM', '9PM', '10PM', '11PM') )
'''


ind = np.arange(7)
for i in np.arange(len(daycounts)):
    daycounts[i] = daycounts[i]
rects2 = ax.bar(ind, daycounts, width, color = 'g')
ax.set_xticklabels( ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday') )
ax.set_ylabel('Total count of crimes')
ax.set_xlabel('Day of Week')
ax.set_title('Total count of crimes by day of week')
ax.set_xticks(ind+width)


'''
ind = np.arange(12)
rects3 = ax.bar(ind, daycounts, width, color = 'b')
ax.set_ylabel('Average daily crime count')
ax.set_xlabel('Month')
ax.set_title('Average daily crime numbers by month')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')  )
'''
plt.show()