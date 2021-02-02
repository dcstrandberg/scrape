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
from pathlib import Path



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
    #Let's think about this -- what are the things we want to know
    #For each keyword, what were the top 10 brands in terms of: 
    #### Average Rank
    #### Count of items on first page
    
    #For first page, what are the breakdowns of Sponsored vs Non-Sponsored
    #### By brand
    #### Weighted (ie the higher the rank, the more weighted the entry is)
    #### Percentages & Counts

    ###Calculate Promoted Ratio for each brand, and rank


    #keywordOfInterest = 'Cereal'
    startDate = '2021-01-01' #date(2021, 1, 1)
    endDate = '2021-01-15'#date(2021, 1, 15)

    figPath = Path("./outputs/")



    df = pd.DataFrame( pull_all_data() )

    keywordList = list( set( df['Keyword'] ))

    for aKey in keywordList: 
        #Clear off the plot so we can plot new stuff on it
        plt.clf()

        #For each keyword, let's get the data 
        keyDF = df[  df['Keyword'] == aKey ].copy()
        keyDF = keyDF[ keyDF['Date'] >= startDate ]
        keyDF = keyDF[ keyDF['Date'] < endDate ]
        keyDF = keyDF[ keyDF['Brand'] != ""]

        
        #Let's first figure out how many items by brand are on page q1
        pagePivot = pd.pivot_table(keyDF, values='Page', index='Brand', aggfunc=np.count_nonzero, fill_value=0)
        #pagePivot = pagePivot.reset_index()


        #rankedDF = defineTrueRankings(keyDF)

        #I think essentially now we just need to make lots of pivots?
        if len(pagePivot.values) > 0:

            if 'Page' in pagePivot.columns: 
                pagePivot.sort_values('Page', ascending=False, inplace=True)
                headPivot = pagePivot.head(10)
                
                xBrand = list( headPivot['Page'] )

                yBrand = list( np.arange(len(headPivot.index))[::-1])
                labels = headPivot.index
                
                plt.rcParams.update({'font.size': 22})
                plt.title("Keyword: " + aKey)
                plt.barh(yBrand, xBrand, align='center', alpha=0.5)
                plt.yticks(yBrand, labels)
                plt.xlabel("Item Count")

                #zip joins x & y coords
                i = 0
                for x, y in zip(xBrand, yBrand): 
                    
                    label = x

                    plt.annotate(
                        label,
                        (x, y), 
                        textcoords="offset points",
                        xytext=(10, -5),
                        ha="center"
                    )
                    i += 1

               
               
                
                #To make it so that no text is cut off
                plt.tight_layout()

                
                tempPath = figPath / ( aKey + " - Brands on P1, " + startDate + " - " + endDate + ".png" )

                #Save the figure
                plt.savefig(tempPath)
                print("Just saved fig: " + aKey)

                #print(pagePivot.describe())

            else: 
                print("Cannot find page: ", aKey)
        else: 
            print("No data for: " + aKey)

    #Now we gotta have a function that does some sort of analysis 
    #I'm thinking a pivot tale  
    #Now we've got the dataframe, we need to process it
    #First we gotta sort it and get the true ranks


    #print(rankedDF.head())

    #Now let's Pivot the table
    #pandas.pivot_table(data, values=None, index=None, columns=None, aggfunc='mean', fill_value=None, margins=False, dropna=True, margins_name='All', observed=False)
    #pivotDF = pd.pivot_table(rankedDF, 'TrueRank', ['Source', 'Keyword', 'Product_Name'], None, 'mean')
    #sponsoredPivotDF = pd.pivot_table(rankedDF, 'TrueRank', ['Source', 'Keyword', 'Product_Name', 'Sponsored'], None, 'mean')
    #keyWordPivots = {}


    #OK so now we've got a pivot table -- Not sure how to filter the data? 
    #plotDictBrand = avgRankAndCount(rankedDF, keywordOfInterest, True)
    #print("First plot")
    #plotDictProd = avgRankAndCount(rankedDF, keywordOfInterest, True)
    #print("Second plot")

    #xBrand = plotDictBrand['x']
    #yBrand = plotDictBrand['y']

    #plt.title("Avg Rank vs Count by Brand - Keyword: " + keywordOfInterest)
    #plt.scatter(xBrand, yBrand)

    #zip joins x & y coords
    #i = 0
    #for x, y in zip(xBrand, yBrand): 
        #label = plotDictBrand['data-label'][i]

        #if y > 0.8 * plotDictBrand['maxY']:
            #plt.annotate(
                #label,
                #(x, y), 
                #textcoords="offset points",
                #xytext=(0, 10),
                #ha="center"
            #)
        #i += 1

    #plt.autoscale()
    
    #plt.show()

    #print(plotDictBrand['x'][0:10])
    #print(plotDictBrand['y'][0:10])




#Ok now we've got a lot of xs and ys -- let's 
   
   #Now we've got all the data into the various things... I guess the next step is to average & count




    #So now we have a keywordDict of arrays for which to put in the true ranks of certain products

    #Now we need to check out the pivots
    #print(pivotDF.head())




    ##datastructure will be productName, searcTerm, non-promo rankList, promo rankList



