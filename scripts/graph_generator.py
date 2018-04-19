
import ujson
import numpy as np
# import seaborn as sns
import pandas as pd
import string
import matplotlib
import matplotlib.ticker as ticker

# matplotlib.use('WX')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib.dates as mdates

from scipy import stats
# import matplotlib.mlab as mlab

def one_sample_tTest(X):
    mean = np.mean(X)
    std = np.std(X)
    count = len(X)
    stErr = std/np.sqrt(count)
    tStat = mean/stErr
    df = count - 1

    # p-value after comparison with the t
    p = 1 - stats.t.cdf(tStat, df=df)
    return p


#load the data
path = '/Users/alessandro/Documents/PhD/userstats'
file_1 = path + '/WDataStats_all.json'
wdStats = pd.read_json(file_1, orient='index')

# file_1 = path + '/WDataStats_1.json'
# wdStats = pd.read_json(file_1, orient='index')
#
# file_2 = path + '/WDataStats_2.json'
# wdStats_2 = pd.read_json(file_2, orient='index')
#
file_3 = path + '/WDepth_new.json'
wdStats_3 = pd.read_json(file_3, orient='index')

file_4 = path + '/WDataStats_RR-temp.json'
wdStats_4 = pd.read_json(file_4, orient='index')
#
# wdStats = pd.concat([wdStats, wdStats_2], axis=0)
# wdStats.drop('avgDepth', axis = 1, inplace=True)
# wdStats = pd.concat([wdStats, wdStats_3], axis=0)

wdStats = wdStats.fillna(0)
wdStats.reset_index(inplace=True)
wdStats['timeframe'] = pd.to_datetime(wdStats['index'])
wdStats.timeframe = wdStats.timeframe - pd.DateOffset(months=1)
wdStats.sort_values(by='timeframe', inplace=True)
wdStats['month'] = wdStats['timeframe'].apply(lambda x: x.strftime('%B %Y'))



wdStats_3 = wdStats_3.fillna(0)
wdStats_3.reset_index(inplace=True)
wdStats_3['timeframe'] = pd.to_datetime(wdStats_3['index'])
wdStats_3.timeframe = wdStats_3.timeframe - pd.DateOffset(months=1)
wdStats_3.sort_values(by='timeframe', inplace=True)
wdStats_3['month'] = wdStats_3['timeframe'].apply(lambda x: x.strftime('%B %Y'))

wdStats_4 = wdStats_4.fillna(0)
wdStats_4.reset_index(inplace=True)
wdStats_4['timeframe'] = pd.to_datetime(wdStats_4['index'])
wdStats_4.timeframe = wdStats_4.timeframe - pd.DateOffset(months=1)
wdStats_4.sort_values(by='timeframe', inplace=True)
wdStats_4['month'] = wdStats_4['timeframe'].apply(lambda x: x.strftime('%B %Y'))

wdStats['noInstances'] = wdStats['avgPop'] * wdStats['noClasses']
wdStats['trueRichness'] = wdStats['classesWInstances']/wdStats['noClasses']

# wdStats.timeframe = wdStats.timeframe - pd.DateOffset(months=1)
# wdStats_4.timeframe = wdStats_4.timeframe - pd.DateOffset(months=1)
# wdStats_3.timeframe = wdStats_3.timeframe - pd.DateOffset(months=1)

wdStats = wdStats.loc[wdStats['timeframe'] > '2013-02-01', ]
wdStats_3 = wdStats_3.loc[wdStats_3['timeframe'] > '2013-02-01', ]
wdStats_4 = wdStats_4.loc[wdStats_4['timeframe'] > '2013-02-01', ]

# ###create grid
# g = sns.FacetGrid(wdStats, col=['avgDepth', 'iRichness', 'cRichness'], hue=['avgDepth', 'iRichness', 'cRichness'], col_wrap=3, )
#
#
#
# ###generate the graphs
#
# plt.plot( 'timeframe', 'avgDepth', data=wdStats, marker='', color='olive', linewidth=2,  label="Avg. depth")
# # plt.plot( 'timeframe', 'avgPop', data=wdStats, marker='', color='olive', linewidth=2, linestyle='dashed', label="Avg. population")
# plt.plot( 'timeframe', 'cRichness', data=wdStats, marker='', color='olive', linewidth=2, linestyle='dashed', label="Class Richness")
# plt.plot( 'timeframe', 'iRichness', data=wdStats, marker='', color='olive', linewidth=2, linestyle='-.', label="Inheritance Richness")
# plt.legend()
#
###test statistical significance of trend
colTested = [ 'avgPop',
       'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses',
       'noLeaf', 'noProps', 'noRoot',
       'timeframe', 'noInstances', 'trueRichness']

wdTrends = wdStats[colTested]
wdTrends[['avgPop', 'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses', 'noLeaf',
       'noProps', 'noRoot', 'noInstances', 'trueRichness']] = wdTrends[['avgPop', 'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses', 'noLeaf',
       'noProps', 'noRoot',  'noInstances', 'trueRichness']] /wdTrends[['avgPop', 'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses', 'noLeaf',
       'noProps', 'noRoot', 'noInstances', 'trueRichness']].shift(1)
wdTrends[['avgPop', 'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses', 'noLeaf',
       'noProps', 'noRoot', 'noInstances', 'trueRichness']] = wdTrends[['avgPop', 'childLessClasses', 'classesWInstances', 'iRichness',
       'maxPop', 'medianInheritance', 'medianPop', 'noClasses', 'noLeaf',
       'noProps', 'noRoot',  'noInstances', 'trueRichness']].apply(lambda x : np.log(x), axis =1)
wdTrends = wdTrends[wdTrends.notnull()]
wdTrends = wdTrends.replace([np.inf, -np.inf], np.nan)
wdTrends = wdTrends.fillna(0)


wdTrends_4 = wdStats_4[['avgRichness', 'maxRichness', 'medianRichness',
       'quantileRichness', 'relRichness', 'timeframe']]
wdTrends_4[['avgRichness', 'maxRichness', 'medianRichness',
       'relRichness']] = wdTrends_4[['avgRichness', 'maxRichness', 'medianRichness',
       'relRichness']]/wdTrends_4[['avgRichness', 'maxRichness', 'medianRichness',
       'relRichness']].shift(1)
wdTrends_4[['avgRichness', 'maxRichness', 'medianRichness',
            'relRichness']]= wdTrends_4[['avgRichness', 'maxRichness', 'medianRichness',
       'relRichness']].apply(lambda x : np.log(x), axis =1)
wdTrends_4 = wdTrends_4[wdTrends_4.notnull()]
wdTrends_4 = wdTrends_4.replace([np.inf, -np.inf], np.nan)
wdTrends_4 = wdTrends_4.fillna(0)

wdTrends_3 = wdStats_3[['avgDepth', 'maxDepth', 'medianDepth','timeframe']]
wdTrends_3[['avgDepth', 'maxDepth', 'medianDepth']] = wdTrends_3[['avgDepth', 'maxDepth', 'medianDepth']]/wdTrends_3[['avgDepth', 'maxDepth', 'medianDepth']].shift(1)
wdTrends_3[['avgDepth', 'maxDepth', 'medianDepth']] = wdTrends_3[['avgDepth', 'maxDepth', 'medianDepth']].apply(lambda x : np.log(x), axis =1)
wdTrends_3 = wdTrends_3[wdTrends_3.notnull()]
wdTrends_3 = wdTrends_3.replace([np.inf, -np.inf], np.nan)
wdTrends_4 = wdTrends_3.fillna(0)


    # .rolling(window=2, center=False).apply(lambda x: np.log(x/x.shift(1)))


###another one

# from itertools import izip,chain

def myticks(x, pos):

    if x == 0: return "$0$"

    # exponent = int(np.log10(100000))
    # coeff = x/10**exponent
    coeff = x/10000


    # return r"${:2.0f}\times 10^{{{:2d}}}$".format(coeff,exponent)
    return int(coeff)

def myticks_prop(x, pos):

    if x == 0: return "$0$"

    # exponent = int(np.log10(100000))
    # coeff = x/10**exponent
    coeff = x/100
    coeff = int(coeff)


    # return r"${:2.0f}\times 10^{{{:2d}}}$".format(coeff,exponent)
    return coeff

def myticks_root(x, pos):

    if x == 0: return "$0$"

    # exponent = int(np.log10(100000))
    # coeff = x/10**exponent
    coeff = x/1000
    coeff = int(coeff)


    # return r"${:2.0f}\times 10^{{{:2d}}}$".format(coeff,exponent)
    return coeff

###no classes
f2 = plt.figure(figsize=(26,4))
font = {'size': 12}

matplotlib.rc('font', **font)

ax1 = plt.subplot(131)
ax1.plot(wdStats['timeframe'],wdStats['noInstances'])
ax1.grid(color='gray', linestyle='--', linewidth=.5)
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(myticks))
ax1.set_ylabel(r'Instances ($noi*10^4$)')

ax2= plt.subplot(132)
ax2.plot(wdStats['timeframe'],wdStats['noClasses'], '-')
ax2.plot(wdStats['timeframe'],wdStats['noRoot'], '--')
ax2.plot(wdStats['timeframe'],wdStats['noLeaf'], ':')
ax2.grid(color='gray', linestyle='--', linewidth=.5)
ax2.legend([r'$noc$', r'$norc$', r'$nolc$'])
ax2.yaxis.set_major_formatter(ticker.FuncFormatter(myticks))
ax2.set_ylabel(r'Classes ($n*10^4$)')


ax3 = plt.subplot(133)
ax3.plot(wdStats['timeframe'],wdStats['noProps'])
ax3.grid(color='gray', linestyle='--', linewidth=.5)
ax3.yaxis.set_major_formatter(ticker.FuncFormatter(myticks_prop))
ax3.set_ylabel(r'Properties ($nop*10^2$)')

ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting

f2.autofmt_xdate()
plt.tight_layout()
plt.show()
plt.savefig('ontocounts.eps', format='eps', transparent=True)

# ax.set_aspect('equal')
# plt.axes().set_aspect('equal')


# f.legend([ax4], 'cosi')
# plt.ylim(-width)
# plt.yticks(range(length), domains[0][0:length])

# plt.tight_layout()
f1 = plt.figure(figsize=(20,8))
font = {'size': 12}

matplotlib.rc('font', **font)

ax1 = plt.subplot(221)
ax1.plot(wdStats['timeframe'],wdStats['avgPop'],  marker='.', markevery=0.05)
ax1.plot(wdStats['timeframe'],wdStats['medianPop'], marker='x', markevery=0.05)
ax1.grid(color='gray', linestyle='--', linewidth=.5)
ax1.legend([r'Avg. population ($ap$)', r'Median population ($mp$)'])
ax1.set_ylabel('Population')
# ax1.yaxis.set_label_position('top')

ax2 = plt.subplot(222)
ax2.plot(wdStats['timeframe'], wdStats['trueRichness'])
ax2.grid(color='gray', linestyle='--', linewidth=.5)
ax2.set_ylabel(r'Class richness ($cr$)')

ax3 = plt.subplot(223)
ax3.plot(wdStats['timeframe'],wdStats['iRichness'],  marker='.', markevery=0.05)
ax3.plot(wdStats['timeframe'],wdStats['medianInheritance'],  marker='x', markevery=0.05)
ax3.grid(color='gray', linestyle='--', linewidth=.5)
ax3.legend([r'Inheritance richness ($ir$)', r'Median inheritance ($mir$)'])
ax3.set_ylabel('Inheritance values')

ax4 = plt.subplot(224)
ax4.plot(wdStats_4['timeframe'],wdStats_4['relRichness'])
ax4.grid(color='gray', linestyle='--', linewidth=.5)
ax4.set_ylabel(r'Relationship richness ($rr$)')


ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting


f1.autofmt_xdate()
plt.tight_layout()
plt.show()
plt.savefig('ontometrics.eps', format='eps', transparent=True)

###depth graph
f3 = plt.figure(figsize=(10,6))
font = {'size': 12}

matplotlib.rc('font', **font)

ax5 = plt.subplot(111)
ax5.plot(wdStats_3['timeframe'],wdStats_3['maxDepth'], marker='^', markevery=0.05)
ax5.plot(wdStats_3['timeframe'],wdStats_3['avgDepth'],  marker='.', markevery=0.05)
ax5.plot(wdStats_3['timeframe'],wdStats_3['medianDepth'], marker='x', markevery=0.05)
ax5.grid(color='gray', linestyle='--', linewidth=.5)
ax5.legend([r'Max depth ($Mdosh$)', r'Avg. depth ($adosh$)', r'Median depth ($mdosh$)'])
ax5.set_ylabel('Ontology depth')

# minlocator = matdates.MinuteLocator(byminute=range(60))  # range(60) is the default

# seclocator.MAXTICKS  = 40000
# minlocator.MAXTICKS  = 40000
ax5.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[6,12]))   #to get a tick every 15 minutes
ax5.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting

f3.autofmt_xdate()

plt.tight_layout()
plt.show()
plt.savefig('ontodepth.eps', format='eps', transparent=True)

# ax6 = plt.subplot(233)
# ax6.plot(wdStats['timeframe'],wdStats['noProps'])
# ax6.grid(color='gray', linestyle='--', linewidth=.5)
# ax6.set_ylabel(r'Properties ($n*10^2$)')
# ax6.yaxis.set_major_formatter(ticker.FuncFormatter(myticks_prop))

#
# ax7.plot(wdStats['timeframe'],wdStats['noClasses'])
# ax7.grid(color='gray', linestyle='--', linewidth=.5)
# ax7.set_ylabel(r'Classes ($n*10^4$)')
# ax7.yaxis.set_major_formatter(ticker.FuncFormatter(myticks))
#
# ax8.plot(wdStats['timeframe'],wdStats['noLeaf'])
# ax8.grid(color='gray', linestyle='--', linewidth=.5)
# ax8.set_ylabel(r'Leaf classes ($n*10^4$)')
# ax8.yaxis.set_major_formatter(ticker.FuncFormatter(myticks))
#
# ax9.plot(wdStats['timeframe'],wdStats['noRoot'])
# ax9.grid(color='gray', linestyle='--', linewidth=.5)
# ax9.set_ylabel(r'Root classes ($n*10^3$)')
# ax9.yaxis.set_major_formatter(ticker.FuncFormatter(myticks_root))



# plt.grid(which='both')
# plt.xlabel(r'$\%$ on the Total Number of Matches')



# # locator = mdates.AutoDateLocator()
# ax1.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax2.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax3.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax4.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax5.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax6.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax7.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax8.fmt_xdata = mdates.DateFormatter('%B %Y')
# ax9.fmt_xdata = mdates.DateFormatter('%B %Y')

ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax5.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax5.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting
ax6.xaxis.set_major_locator(mdates.MonthLocator(interval=6))   #to get a tick every 15 minutes
ax6.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))     #optional formatting

f.autofmt_xdate()


# f.legend([ax4], 'cosi')
# plt.ylim(-width)
# plt.yticks(range(length), domains[0][0:length])
plt.tight_layout()
plt.savefig("ontometrics.pdf", format="pdf")

plt.savefig('ontometrics.eps', format='eps', transparent=True)

plt.show()


