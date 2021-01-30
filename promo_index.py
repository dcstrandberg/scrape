#At some point we need to go through the database and normalize the page  ranks
####This will entail counting the number of entries per source / day and appending a real ranking to them
#Make a plugin that checks if something is wrong or untrue, or may be false or something


from datetime import date
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import re
import random
import pandas as pd
from multiprocessing import Process, Queue, Pool, Manager, Lock
from DB_functions import pull_all_data


#Also need to make sure that we only pull things from the DB that aren't currently "Truly Ranked"
def defineTrueRankings(df):
    #Ok let's figure out what we want to do about this
    #We wanna group every unique source, date, keyword, and page
    #Then we need to loop through all the products within that group, and rank them
    #Ranking them will involve checking the rank-of-interest against the current list of currently ranked data

    #First we gotta split the df into dates/sources/keywords
    normalRank = []

    #Not sure what to do about this...
    rankedDF = df.sort_values(['Source', 'Date', 'Keyword', 'Page', 'List_Position'], 0, True)
    
    trueRank = 1
    oldPage = 1

    for i in range(len(rankedDF['Source'])): 
        if rankedDF['Page'][i] != oldPage: 
            trueRank = 1
            normalRank.append(trueRank)
        else: 
            normalRank.append(trueRank)
        
        trueRank += 1
        oldPage = df['Page'][i]
    
    #Now we need to append this to the df and return the df
    df.insert( len(df.columns), 'TrueRank', normalRank )
    return df

def avgRankAndCount(df, filterKey, isProd = False):
    #Filter key can be an array or just one key

    #Let's see if I can just do this using the pandas pivot_table method
    if isProd:
        pivotAvg = pd.pivot_table(df, values = "TrueRank", index = "Product_Name", columns = "Keyword")
        pivotCount = pd.pivot_table(df, values = "TrueRank", index = "Product_Name", columns = "Keyword", aggfunc = np.count_nonzero)

        #pivotAvg = pivotAvg[pivotAvg['Keyword'] in filterKey]
        #pivotCount = pivotCount[pivotCount['Keyword'] in filterKey]
    else:
        pivotAvg = pd.pivot_table(df, values = "TrueRank", index = "Brand", columns = "Keyword")
        pivotCount = pd.pivot_table(df, values = "TrueRank", index = "Brand", columns = "Keyword", aggfunc = np.count_nonzero)

        #pivotAvg = pivotAvg[pivotAvg['Keyword'] in filterKey]
        #pivotCount = pivotCount[pivotCount['Keyword'] in filterKey]


    #So now we have the xs and ys...

    #I guess we now return the x & y arrays and the laels for each?
    #We gotta declare a holder
    returnDict = {
        'x': [],
        'y': [],
        'data-label': [], 
        'maxX': 0,
        'maxY': 0
    }

               
    tempx = pivotAvg.reset_index()
    tempy = pivotCount.reset_index()
    
    
    returnDict['x'] = tempx[filterKey]
    returnDict['y'] = tempy[filterKey]

    returnDict['maxX'] = np.amax(tempx[filterKey])
    returnDict['maxY'] = np.amax(tempy[filterKey])
    
    tempLabel = pivotAvg.reset_index()
    if isProd: 
        returnDict['data-label'] = tempLabel['Product_Name']
    else:
        returnDict['data-label'] = tempLabel['Brand']

    return returnDict


if __name__ == "__main__":

    keywordOfInterest = 'Cereal'

    df = pd.DataFrame( pull_all_data() )

    #Now we gotta have a function that does some sort of analysis 
    #I'm thinking a pivot tale  
    #Now we've got the dataframe, we need to process it
    #First we gotta sort it and get the true ranks

    rankedDF = defineTrueRankings(df)
    #print(rankedDF.head())

    #Now let's Pivot the table
    #pandas.pivot_table(data, values=None, index=None, columns=None, aggfunc='mean', fill_value=None, margins=False, dropna=True, margins_name='All', observed=False)
    #pivotDF = pd.pivot_table(rankedDF, 'TrueRank', ['Source', 'Keyword', 'Product_Name'], None, 'mean')
    #sponsoredPivotDF = pd.pivot_table(rankedDF, 'TrueRank', ['Source', 'Keyword', 'Product_Name', 'Sponsored'], None, 'mean')
    #keyWordPivots = {}


    #OK so now we've got a pivot table -- Not sure how to filter the data? 
    plotDictBrand = avgRankAndCount(rankedDF, keywordOfInterest, True)
    print("First plot")
    #plotDictProd = avgRankAndCount(rankedDF, keywordOfInterest, True)
    #print("Second plot")

    xBrand = plotDictBrand['x']
    yBrand = plotDictBrand['y']

    plt.title("Avg Rank vs Count by Brand - Keyword: " + keywordOfInterest)
    plt.scatter(xBrand, yBrand)

    #zip joins x & y coords
    i = 0
    for x, y in zip(xBrand, yBrand): 
        label = plotDictBrand['data-label'][i]

        if y > 0.8 * plotDictBrand['maxY']:
            plt.annotate(
                label,
                (x, y), 
                textcoords="offset points",
                xytext=(0, 10),
                ha="center"
            )
        i += 1

    plt.autoscale()
    
    plt.show()

    print(plotDictBrand['x'][0:10])
    print(plotDictBrand['y'][0:10])




#Ok now we've got a lot of xs and ys -- let's 
   
   #Now we've got all the data into the various things... I guess the next step is to average & count




    #So now we have a keywordDict of arrays for which to put in the true ranks of certain products

    #Now we need to check out the pivots
    #print(pivotDF.head())




    ##datastructure will be productName, searcTerm, non-promo rankList, promo rankList



