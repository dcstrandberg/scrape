#File for tagging data based on product name
#Currently super inefficient because we're searching through the full CSV file each time instead of indexing the CSV files and iterating over the words in the product name.....
 
#TODOS:
#Clean up numbers that are saved, so it includes units too
#For some reason, the 5th entry is getting tagged as a mispelled Q water brand, even though it's stubborn soda
###Also need to get it so that the correct sub-brands get chosen instead of the least interesting top-brand (i.e. Mountain Dew Kickstart instead of mountain Dew)
#Fix pack type so that it's catching Cans and whatnot -- not catching Mini Cans
###Think we need to use the partial match on those, which we can't do in the brand names, but in this instance, I think it could work

#Add Variant and MFG support
#Keep a metadata.txt file that says what Packages I've downloaded and are using
###Currently we've got pandas, fuzzywuzzy, which also needs a C-Compiler to run properly, beautifulSoup, pathlib

import pandas as pd
import re
from pathlib import Path
from fuzzywuzzy import process

def tag_data(productName):

    #Clean up the product name
    #First make it lower case
    productName = productName.lower()

    #Then check it for starting and ending quotes:
    if productName[0] == '"': productName = productName[1:]
    if productName[-1] == '"': productName = productName[:-1]


    #figure out the necessary filepaths
    data_folder = Path("./tagging_csvs/")

    brandMFGDict = pd.read_csv(data_folder / 'brand & MFG.csv')
    #variantDict = pd.read_csv(data_folder / 'variant.csv')
    packDict = pd.read_csv(data_folder / 'pack.csv')


    #Loop through the words in the product name and determine whether they're a brand, variant, pack type, size, or count
    #for word in productName.split():
    #    if word in brandMFGDict['Brand'] or        

    #Set all the default values so if we pass data back, it's not None, it's ""
    brand = ""
    mfg = ""
    variant = ""
    packType = ""
    packSize = ""
    packCount = ""

    #For now let's actually do it the hard way and loop through each of the CSV lists to see if there's anything -- this prevents having to do any special string formatting
    bestScores = [score[1] for score in process.extractBests(productName, brandMFGDict['Brand'])]
    bestMatches = process.extractBests(productName, brandMFGDict['Brand'])
    
    #We're not using the built in function extractBest because we need to be able to discern between matches that have the same score, but one is longer than the other
    if bestScores.count(max(bestScores)) > 1: 
        bestIndex = 0
        matchLength = 0
        for i in range(len(bestScores)):
            #Check if the latest entry is longer than the current best match
            currentMatch = bestMatches[i]
            if bestScores[i] == max(bestScores) and len(currentMatch[0]) > matchLength:
                bestIndex = i
                matchLength = len(currentMatch[0])
        bestMatch = bestMatches[bestIndex]
    else:
        bestMatch = bestMatches[bestScores.index(max(bestScores))]

    if bestMatch[1] > 70: brand = bestMatch[0]
    
    #Set bestSimilarity to 0, for improvement
    #bestSimilarity = 0
    #for i in range(len(brandMFGDict['Brand'])):        
    #    if brandMFGDict['Brand'][i].lower() in productName.lower(): 
    #        brand = brandMFGDict['Brand'][i]
            #mfg = brandMFGDict['Manufacturer'][i]
    #        break
    
    #for i in range(len(variantDict['Variant'])):
    #    if variantDict['Variant'][i].lower() in productName.lower():
    #        variant = variantDict['Variant'][i]
    #        break

    for i in range(len(packDict['Pack Type'])):
        if packDict['Pack Type'][i].lower() in productName.lower():
            packType = packDict['Pack Type'][i]
            break

    #pack size and count need to be done differently since it's not looking up possible names, it's parsing the string to look for words 
        
    #Break the product name into words
    splitName = productName.split()
    unitsList = ['oz', 'kg', 'gram', 'lb', 'kilo', 'ml', 'liter', 'pound', 'fluid', 'ounce', 'kilogram', 'fl']

    #unitRegex = "r'.*" + ".*|.*".join([item for item in unitsList] + ".*'"

    for i in range(len(splitName)):
        #This is the list of all the sizes I can think of...
        if (True in [(unit in splitName[i].lower()) for unit in unitsList]):
            
            #Now check if the previous, current, or next word has #s in it
            #Range goes a few words before and after to look for "x fl oz" or "lbs = 5" or something stupid like that
            ####I think I need to fix this so that it captures numbers that are part of the same word as "Pack" etc. 
            ####And I think I can fix both of these by implementing something along the lines of "Pack of \d" and similar wording... I'll see
            ####I'm looking into fuzzy logic, but I think that 's probably overkill...
            for j in range(-2, 3):
                isMatch = re.search(r'\d+\.*\d*', splitName[i+j].lower())
                if isMatch: 
                    #This is a really clunky way to clean the data and get only the numbers and then the words
                    #THERE SURELY MUST BE A BETTER WAY
                    numSearch = re.search(r'(\d+\.*\d*)', splitName[i+j])
                    if numSearch: relevantInfo = [numSearch.group(1)]
                    #relevantInfo = [splitName[i+j]]
                    relevantInfo += [re.sub(r'[0-9]|[-/()\,]',r'', word) for word in splitName[i+j: i+j+3] if re.sub(r'[0-9]|[-/()\,]',r'', word) in unitsList]

                    packSize = re.sub(r'[-/()]|\sBD',r'', " ".join(relevantInfo)).lower()
                    break
        
        #Now check the pack count
        if "count" in splitName[i].lower() or "pack" in splitName[i].lower() or "pk" in splitName[i].lower() or "ct" in splitName[i].lower():

            #Now check to see if there are #s in the words before or after
            for j in range(-2, 3):
                isMatch = re.search(r'\d+', splitName[i+j].lower())
                if isMatch: 
                    packCount = re.sub(r'[,-./()]|\sBD',r'', splitName[i+j].lower())
                    break
    
    #return the dictionary
    return {'brand':brand, 'pack Type':packType, 'count':packCount, 'size':packSize}#'mfg':mfg, 'variant':variant, 'packType':packType, 'count': packCount, 'size': packSize}


#Now for debugging 
#let's open a CSV file and attempt to tag a couple things
amazonFolder = Path('./amazon_data/')
scrapeData = pd.read_csv(amazonFolder / '2020-11-25-SearchList.csv')

productList = scrapeData['Product Name']

for i in range(5):
    print(productList[i])
    print(tag_data(productList[i]))
