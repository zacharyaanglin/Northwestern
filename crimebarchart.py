# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 10:10:23 2015

@author: zacha_000
"""
import numpy as np
import matplotlib.pyplot as plt

jancount = 463497
febcount = 407957
marcount = 492905
aprcount = 494231
maycount = 521509
juncount = 504706
julcount = 531295
augcount = 524592
sepcount = 492694
octcount = 500018
novcount = 448726
deccount = 419714

janavg = jancount / 15
febavg = febcount / 15
maravg = marcount / 15
apravg = aprcount / 15
mayavg = maycount / 15
junavg = juncount / 14
julavg = julcount / 14
augavg = augcount / 14
sepavg = sepcount / 14
octavg = octcount / 14
novavg = novcount / 14
decavg = deccount / 14

monthavg = (janavg, febavg, maravg, apravg, mayavg, junavg, julavg, augavg, sepavg, octavg, novavg, decavg)

janday = jancount / 465
febday = febcount / 423#account for leap days in 2004, 2008, 2012
marday = maravg / 31
aprday = apravg / 30
mayday = mayavg / 31
junday = junavg / 30
julday = julavg / 31
augday = augavg / 31
sepday = sepavg / 30
octday = octavg / 31
novday = novavg / 30
decday = decavg / 31

dayavg = (janday, febday, marday, aprday, mayday, junday, julday, augday, sepday, octday, novday, decday)

ind = np.arange(12)
width = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, monthavg, width, color = 'r')
#rects2 = ax.bar(ind+width, dayavg, width, color = 'g')

ax.set_ylabel('Average crime count')
ax.set_xlabel('Month')
ax.set_title('Average count of crimes by month')
ax.set_xticks(ind+width)
ax.set_xticklabels( ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec') )

#ax.legend( (rects1[0], rects2[0]), ('Month', 'Day') )

plt.show()