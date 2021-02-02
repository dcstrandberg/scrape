#File for tagging data based on product name
#Currently super inefficient because we're searching through the full CSV file each time instead of indexing the CSV files and iterating over the words in the product name.....
 
#TODOS:
#Pass the data frame to the function so it opens up once and doesn't open/close every time the function is called
#Add Variant and MFG support

import pandas as pd
import re
from pathlib import Path
from fuzzywuzzy import process, fuzz
from datetime import date
import DB_functions

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
    #bestScores = [score[1] for score in process.extractBests(productName, brandMFGDict['Brand'])]
    #bestMatches = process.extractBests(productName, brandMFGDict['Brand'])

    bestScores = [fuzz.token_set_ratio(productName, score) for score in brandMFGDict['Brand']]
    bestMatches = brandMFGDict['Brand']

    sodaScore = fuzz.token_set_ratio(productName, 'soda')
    #print(bestMatches)

    #We're not using the built in function extractBest because we need to be able to discern between matches that have the same score, but one is longer than the other
    #Also checking if the match is equally good to the word "Soda", given that that's a huge issue with our database
    if bestScores.count(max(bestScores)) > 1 or max(bestScores) == sodaScore: 
        bestIndex = 0
        matchLength = 0
        for i in range(len(bestScores)):
            #Check if the latest entry is longer than the current best match
            currentMatch = bestMatches[i]
            #print("Brand Matches: ", currentMatch)
            if bestScores[i] == max(bestScores) and len(currentMatch) > matchLength:
                bestIndex = i
                matchLength = len(currentMatch)
        bestMatch = bestMatches[bestIndex]
    else:
        bestIndex = bestScores.index(max(bestScores))
        bestMatch = bestMatches[bestIndex]

    if bestScores[bestIndex] > 85: 
        brand = bestMatch
    #else: 
        #print("Brand Score: " + str(bestScores[bestIndex]) + " for " + bestMatch)
    # if bestMatch[1] > 85: brand = bestMatch[0]
    
    #Now lookup the MFG that goes along w/ that brand
    if brand in list(brandMFGDict['Brand']):
        brandIndex = list(brandMFGDict['Brand']).index(brand)
        mfg = brandMFGDict['Manufacturer'][brandIndex]
    #else: 
    #    print("Couldn't find MFG for " + brand)    
    
    #And now we do variants
    #bestScores = [score[1] for score in process.extractBests(productName, variantDict['Variant'])]
    #bestMatches = process.extractBests(productName, variantDict['Variant'])

    bestScores = [fuzz.token_set_ratio(productName, score) for score in variantDict['Variant']]
    bestMatches = variantDict['Variant']

    sodaScore = fuzz.token_set_ratio(productName, 'soda')
    #print(bestMatches)

    #We're not using the built in function extractBest because we need to be able to discern between matches that have the same score, but one is longer than the other
    #Also checking if the match is equally good to the word "Soda", given that that's a huge issue with our database
    if bestScores.count(max(bestScores)) > 1 or max(bestScores) == sodaScore or (bestMatches[bestScores.index(max(bestScores))] == "Cream Soda" and "cream" not in productName.lower()): 
        bestIndex = 0
        matchLength = 0
        for i in range(len(bestScores)):
            #Check if the latest entry is longer than the current best match
            currentMatch = bestMatches[i]
            #print("Match: ", currentMatch, bestScores[i])
            #print("Brand Matches: ", currentMatch)
            if bestScores[i] == max(bestScores) and len(currentMatch) > matchLength and not (currentMatch == "Cream Soda" and "cream" not in productName.lower()):
                bestIndex = i
                matchLength = len(currentMatch)
        bestMatch = bestMatches[bestIndex]
    else:
        bestIndex = bestScores.index(max(bestScores))
        bestMatch = bestMatches[bestIndex]

    if bestScores[bestIndex] > 50: 
        variant = bestMatch
    #else:
    #    print("Variant Score: " + str(bestScores[bestIndex]) + " for " + bestMatch)

    # if bestMatch[1] > 85: variant = bestMatch[0]


    #Now do some special string manipulation to get at pack metrics
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
    #Create a list of pack wording regex identifiers
    searchList = [r"\d+.count", r"pack of \d+", r"case of \d+", r"\d+.pack", r"\d+.pk", r"\d+.pck", r"\d+.ct", r"\d+.case"]
    packCount = ""

    for search in searchList:
        pattern = re.compile(search)
        match = re.search(pattern, productName.lower())
        if match:
            if packCount != "":
                packCount += " " + match.group(0)
            else:
                packCount += match.group(0)

    #print("Tagged")
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
        #print("Tagging Entry: " + str(counter))
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
    pathName = re.search(re.compile(r'\d\d\d\d-\d\d-\d\d'), str(readFile))
    if pathName: dateString = pathName.group(0)

    try:
        df.to_csv(filePath /  (dateString + '-SearchList-Tagged2.csv'), index=False, encoding='utf-8')
        print("Saved", filePath /  (dateString + '-SearchList-Tagged2.csv'))
    except:
        print("Error saving tagged file: ", filePath /  (dateString + '-SearchList-Tagged2.csv'))


#def update_database_tags(date = "All", source = "All"): 
    #First check the parameters
    #if date == "All": date = re.compile(r'\d\d\d\d=\d\d-\d\d')
    #if source == "All": source = re.compile(r'.*')



def get_tag_dicts():

    #figure out the necessary filepaths
    data_folder = Path("./tagging_csvs/")

    brandMFGDict = pd.read_csv(data_folder / 'brand & MFG.csv')
    variantDict = pd.read_csv(data_folder / 'variant.csv')
    packDict = pd.read_csv(data_folder / 'pack.csv')

    return (brandMFGDict, variantDict, packDict)

def tagDB(): 
    
    df = DB_functions.pull_all_data()


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
    for name in df['Product_Name']: 
        #print("Tagging Entry: " + str(counter))
        tagDict = tag_data(name, brandMFGDict, variantDict, packDict)

        brands.append(tagDict['brand'])
        MFGs.append(tagDict['mfg'])
        variants.append(tagDict['variant'])
        packTypes.append(tagDict['packType'])
        packCounts.append(tagDict['count'])
        packSizes.append(tagDict['size'])

        if counter % 100 == 0: 
            print("Tagging... {:.2f}%".format( (counter * 100 / len(df['Product_Name']))))
        counter += 1


    #Append the new columns to the dataframe
    df['Brand'] = brands
    df['MFG'] = MFGs
    df['Variant'] = variants
    df['Pack_Type'] = packTypes
    df['Pack_Count'] = packCounts
    df['Pack_Size'] = packSizes

    numWritten = DB_functions.update_scrape_db( df, True)
    print("Entries written to DB: ", numWritten)
    return numWritten


if __name__ == "__main__":

    #ONLY DO THIS IF WE WANT TO COMPLETELY OVERWRITE THE DB
    num = tagDB()
    print("We just tagged " + num + " entries!")
    