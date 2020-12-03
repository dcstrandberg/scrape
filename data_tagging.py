#File for tagging data based on product name
#Currently super inefficient because we're searching through the full CSV file each time instead of indexing the CSV files and iterating over the words in the product name.....
 
#TODOS:
#Pass the data frame to the function so it opens up once and doesn't open/close every time the function is called
#Add Variant and MFG support
#Keep a metadata.txt file that says what Packages I've downloaded and are using
###Currently we've got pandas, fuzzywuzzy, which also needs a C-Compiler to run properly, beautifulSoup, pathlib

import pandas as pd
import re
from pathlib import Path
from fuzzywuzzy import process
from datetime import date

def tag_data(productName, brandMFGDict, variantDict, packDict):

    #Clean up the product name
    #First make it lower case
    productName = productName.lower()

    #Then check it for starting and ending quotes:
    if productName[0] == '"': productName = productName[1:]
    if productName[-1] == '"': productName = productName[:-1]


    #figure out the necessary filepaths
    #data_folder = Path("./tagging_csvs/")

    #brandMFGDict = pd.read_csv(data_folder / 'brand & MFG.csv')
    #variantDict = pd.read_csv(data_folder / 'variant.csv')
    #packDict = pd.read_csv(data_folder / 'pack.csv')


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

    if bestMatch[1] > 85: brand = bestMatch[0]
    
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
            for j in range(max(i - 2, 0), min(i + 3, len(splitName))):
                isMatch = re.search(r'\d+\.*\d*', splitName[j].lower())
                if isMatch: 
                    #This is a really clunky way to clean the data and get only the numbers and then the words
                    #THERE SURELY MUST BE A BETTER WAY
                    numSearch = re.search(r'(\d+\.*\d*)', splitName[j])
                    if numSearch: relevantInfo = [numSearch.group(1)]
                    #relevantInfo = [splitName[j]]
                    relevantInfo += [re.sub(r'[0-9]|[-/()\,]',r'', word) for word in splitName[max(i-2, 0): min(i + 3, len(splitName))] if re.sub(r'[0-9]|[-/()\,]',r'', word) in unitsList]

                    packSize = re.sub(r'[-/()]|\sBD',r'', " ".join(relevantInfo)).lower()
                    break
        
        #Now check the pack count
        if "count" in splitName[i].lower() or "pack" in splitName[i].lower() or "pk" in splitName[i].lower() or "ct" in splitName[i].lower():

            #Now check to see if there are #s in the words before or after
            for j in range(max(i - 2, 0), min(i + 3, len(splitName))):
                isMatch = re.search(r'\d+', splitName[j].lower())
                if isMatch: 
                    packCount = re.sub(r'[,-./()]|\sBD',r'', splitName[j].lower())
                    break
    print("Tagged")
    #return the dictionary
    return {'brand':brand, 'mfg':mfg, 'variant':variant, 'packType':packType, 'count': packCount, 'size': packSize}

def tag_scrape_file(readFile):

    #figure out the necessary filepaths and read in the CSV to a dataframe
    filePath = Path(readFile).parents[0] 
    df = pd.read_csv(readFile)

    #figure out the necessary filepaths
    data_folder = Path("./tagging_csvs/")

    brandMFGDict = pd.read_csv(data_folder / 'brand & MFG.csv')
    variantDict = pd.read_csv(data_folder / 'variant.csv')
    packDict = pd.read_csv(data_folder / 'pack.csv')

    #Declare the lists to hold the tags and the counts to write the 
    brands=[] #List to store tagged brand of the product
    MFGs=[] #List to store tagged manufcaturer of the product
    variants=[] #List to store tagged variants of the product
    packTypes=[] #List to store tagged pack types of the product
    packCounts=[] #List to store tagged pack counts of the product
    packSizes=[] #List to store tagged pack sizes of the product

    #For each line of the name, run the tag_data function and append the tagged data
    counter = 0
    for name in df['Product Name']: 
        print("Tagging Entry: " + str(counter))
        tagDict = tag_data(name, brandMFGDict, variantDict, packDict)

        brands.append(tagDict['brand'])
        MFGs.append(tagDict['mfg'])
        variants.append(tagDict['variant'])
        packTypes.append(tagDict['packType'])
        packCounts.append(tagDict['count'])
        packSizes.append(tagDict['size'])

        counter += 1

    #Append the new columns to the dataframe
    df.insert(len(df.columns), 'Brand', brands)
    df.insert(len(df.columns), 'MFG', MFGs)
    df.insert(len(df.columns), 'Variant', variants)
    df.insert(len(df.columns), 'Pack Type', packTypes)
    df.insert(len(df.columns), 'Pack Count', packCounts)
    df.insert(len(df.columns), 'Pack Size', packSizes)

    #And write the dataframe to the file -- THIS PROBABLY SHOULDN"T BE DONE IN THIS FUNCTION, BUT WITHIN THE CALLING FUCNTION< BUT WHATEVER
    df.to_csv(filePath /  (str(date.today()) + '-SearchList-Tagged.csv'), index=False, encoding='utf-8')

def get_tag_dicts():

    #figure out the necessary filepaths
    data_folder = Path("./tagging_csvs/")

    brandMFGDict = pd.read_csv(data_folder / 'brand & MFG.csv')
    variantDict = pd.read_csv(data_folder / 'variant.csv')
    packDict = pd.read_csv(data_folder / 'pack.csv')

    return (brandMFGDict, variantDict, packDict)


#Now for debugging 
#let's open a CSV file and attempt to tag a couple things
#amazonFolder = Path('./amazon_data/')
#scrapeData = pd.read_csv(amazonFolder / '2020-11-25-SearchList.csv')

#productList = scrapeData['Product Name']

#for i in range(5):
#    print(productList[i])
#    print(tag_data(productList[i]))
#tag_scrape_file('./amazon_data/2020-11-28-SearchList.csv')