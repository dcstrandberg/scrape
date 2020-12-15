#TODO: Add some form of analysis?, 
#Edit proxy cycling, so the process originator loop passes the # of proxy to use
#Additionally, need to add a fail state -- raised exception if there's more than x loops of proxy or captcha cycling....
#Research whether we can pull more than 20 proxyList at once
####Possibly even start writing them to a file? 

#Add reading in the search terms from a file or something, so it's easily editable?
#Add best seller flag?
#Test out using sentiment analysis to tag the data -- brand name / pack size / pack count / $ per oz
#Add more sources: instacart, walmart, etc. Then create wrapper program that calls each of these subroutines

#from proxy_list_scrape import scrapeProxyList, getProxyList, updateProxyFile
from proxy_list_scrape import scrapeProxyList
from datetime import date
import re
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from multiprocessing import Process, Queue, Pool, Manager, Lock
from send_email import send_email
from data_tagging import tag_data, get_tag_dicts
from DB_functions import update_scrape_db

#Declare the variables that will be needed to run the request loop
proxyList = []
#proxyList.append(scrapeProxyList())
proxyCounter = 0
startTime = time.time()
qcount = 0

#Declare the lists that will be used to store the final dataframe
sourceList=[] #list to store sources of scraped data
searchTerms=[] #list to store keyword of product
dates = [] #list to store date of search for product
products=[] #List to store name of the product
prices=[] #List to store price of the product
regularPrices=[] #List to store regular prices, if any
onSales=[] #List to store boolean of whether item is on sale or not
amazonChoices=[] #List to store boolean of whether item is an Amazon Choice product or not
sponsoredList=[] #List to store boolean of whether item is Sponsored
positions=[] #List of where the product was positioned in the search
pages=[] #List to store ratings of the product

#Now declare the lists of additional tag data
brands=[] #List to store tagged brand of the product
MFGs=[] #List to store tagged manufcaturer of the product
variants=[] #List to store tagged variants of the product
packTypes=[] #List to store tagged pack types of the product
packCounts=[] #List to store tagged pack counts of the product
packSizes=[] #List to store tagged pack sizes of the product

#Declare the request variables that determine how many requests will get made -- eventually these will be fed as arguments to the request function from a wrapper function
no_pages = 4
keywords = ['Soda', 'Water', 'Sports Drinks', 'Coffee', 'Cereal', 'Snack Bars', 'Chips', 'Snacks', 'Contact Lenses', 'Coke', 'Fanta', 
    'Sprite', 'Powerade', 'Frosted Flakes', 'Special K', 'Froot Loops', 'Raisin Bran', 'Pringles', 'Cheez It', 'Rice Krispies', 'Rice Krispies Treats', 
    'Pop Tarts', 'Acuvue', 'Oasys', 'Pet Food', 'Dog Food', 'Cat Food']
#keywords = ['cereal']

#THIS IS WHERE I SHOULD DECLARE A FUNCTION TO GENERATE THE PROXY LIST
###THEN IT SHOULD STORE THE GLOBAL LIST
###AND HAVE METHODS TO CALL/UPDATE IT

#Function for getting a proxy
def generateProxyList(lock, proxyCounter):
    #global proxyList
    lock.acquire()
    try:
        proxyList = scrapeProxyList(proxyCounter) 
    finally:
        lock.release()
    return proxyList

def getProxy(proxyList):
    #global proxyList
    print(len(proxyList))
    #Return a random proxy in the list
    return proxyList[0]


#Function for deleting the proxy from
def removeProxy(proxyList, proxy, lock, proxyCounter):
    #global proxyList
    lock.acquire()

    try:
        #Figure out what index in the list is the one to delete
        if proxy in proxyList:
            proxyList.remove(proxy)
            print("Removing " + str(proxy) + ". " + str(len(proxyList)) + " remaining.")# + " from: " + str(proxyList))

        #Check to make sure it's not the only proxy in the list, and if it is, append a new scrape
        if len(proxyList) < 1: 
            print("Proxy List Too Short: " + str(proxyList))
            print("Refreshing proxy List")
            proxyList.extend( scrapeProxyList(proxyCounter) )
            if proxyCounter < 200: proxyCounter += 20
    finally:
        lock.release()
    return proxyCounter


def get_data(keyword, pageNo, q, lock, proxyList, tagging_df, proxyCounter):  
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    
    #Keep trying until an exception isn't raised
    noError = False
    
    while not noError:
        #wait for a random period of time before fetching the next page, to help avoid being blocked by amazon
        time.sleep(random.random())
        proxy = getProxy(proxyList)

        if 'http' in proxy: 
            printProxy = proxy['http']
        else:
            printProxy = proxy['https']

        try:
            r = requests.get("https://www.amazon.com/s?k=" + keyword + "&page=" + str(pageNo), headers=headers, proxies=proxy, timeout=15)
            
        except:
            #remove the bad proxy from the list so we don't try it again
            proxyCounter = removeProxy(proxyList, proxy, lock, proxyCounter)
            print("There was a connection error")
            print("Bad Proxy: " + printProxy)

        else:
            print("Now things are ok")
            #THIS IS ALL DEBUGGING NONSENSE
            if 'http' in proxy: 
                printProxy = proxy['http']
            else:
                printProxy = proxy['https']

            print("This proxy worked " + printProxy)

            #Add here some checking for whether soup is readable

            content = r.content
            soup = BeautifulSoup(content, features="lxml")
            
            if len(soup.findAll('div', attrs={'data-index':re.compile(r'\d+')})) == 0:
                #remove the bad proxy from the list so we don't try it again
                proxyCounter = removeProxy(proxyList, proxy, lock, proxyCounter)

                print("There was a captcha error")
                print("Bad Proxy: " + printProxy)
            else:
                noError = True
        
    #content = r.content
    #soup = BeautifulSoup(content, features="lxml")
    #print(soup.encode('utf-8')) # uncomment this in case there is some non UTF-8 character in the content and
                                 # you get error
	

    for d in soup.findAll('div', attrs={'data-index':re.compile(r'\d+')}):

        #Some of those search results may be carousels, so we need to iterate for each "entry" within the search result
        #Set up a carousel counter to add to placement entries for carousel entries
        carouselCounter = 0      

        if len(d.findAll('div', attrs={'data-asin':re.compile(r'.*')})) > 0:
            soupList = d.findAll('div', attrs={'data-asin':re.compile(r'.*')})
        else:
            soupList = d.findAll('div', attrs={'class':'sg-col-inner'})

        #Now I'm only getting the top carousel data
        for product in soupList:
            #print("We're in the second soup loop")
            #if we're in the first carousel
            if product.find('span', attrs={'data-click-el':'title'}) is not None:
                name = product.find('span', attrs={'data-click-el':'title'})
                ad = True
            else:
                name = product.find('span', attrs={'class':re.compile(r'a-color-base a-text-normal')})
                ad = False
            
            #print(name)
            price = product.find('span', attrs={'class':'a-offscreen'})
            #print(price)
            regularPrice = product.find('span', attrs={'data-a-strike':'true'})

            #This should work, because it's looking 
            amazonChoice = d.find('span', attrs={'class':'a-badge'})
            placement = d['data-index']

            all=[]

            #Product Name and Price should be the two minimum checks before putting ANYTHING into the queue
            #print("Checking Name: ", name)
            if name is not None: #and price is not None:
                
                all.append("Amazon")
                all.append(keyword)
                all.append(date.today())
                
                all.append(name.text)

                if price is not None:
                    all.append(price.text)
                else:
                    all.append("$0")
                
                if regularPrice is not None:
                    all.append(regularPrice.contents[0].text)
                    all.append(True)
                else:
                    all.append('$0')
                    all.append(False)
                
                if amazonChoice is not None:
                    all.append(True)
                else:
                    all.append(False)

                #The second part of the check is to look if something is within a carousel, which I'm considering a sponsored product....
                if 'AdHolder' in d['class'] or 's-widget' in d['class'] or ad == True:
                    all.append(True)
                else:
                    all.append(False)

                #Using string slice to only input the # part of the string and then appending "-#" if it's one of the carousel entries
                all.append( placement + "-" + str(carouselCounter))
                
                all.append(str(pageNo))

                #Now add the tags based on the newly updated data            
                tagDict = tag_data(name.text, tagging_df[0], tagging_df[1], tagging_df[2])
                all.append(tagDict['brand'])
                all.append(tagDict['mfg'])
                all.append(tagDict['variant'])
                all.append(tagDict['packType'])
                all.append(tagDict['count'])
                all.append(tagDict['size'])
            
                q.put(all)
                
                #increment carousel counter -- shouldn't matter if this isn't a carousel
                carouselCounter += 1
             
    print("Put " + keyword + " #" + str(pageNo))
    #DEBUGGING -- if this is page 1 of the first loop, just save the full html file
    #if pageNo == 1 and keyword == 'cereal':
    #    with open('cereal_html.html', 'w', encoding='utf-8') as outfile:
    #        outfile.write(str(soup))


results = []
if __name__ == "__main__":
    print("In the main")
    
    m = Manager()
    q = m.Queue() # use this manager Queue instead of multiprocessing Queue as that causes error
    lock = Lock()

    proxyCounter = 20
    proxyList = generateProxyList(lock, proxyCounter)
    #proxyList.extend( scrapeProxyList() )

    print("ProxyList of length: " + str(len(proxyList)))

    #This is where we create the keyword / page dictionary to loop through, so we can truly be parallel with execution
    searchList = []
    
    #raise SyntaxError

    for word in keywords:
        for i in range (1, no_pages):
            searchList.append( {'word': word, 'page': i} )

    #get the path for the tagging dataframes
    tagging_df = get_tag_dicts()

    p = {}    

    for i in range(len(searchList)):            
        print("starting process: ", i)
        p[i] = Process(target=get_data, args=(searchList[i]['word'], searchList[i]['page'], q, lock, proxyList, tagging_df, proxyCounter))
        p[i].start()

    
        # join should be done in seperate for loop 
        # reason being that once we join within previous for loop, join for p1 will start working
        # and hence will not allow the code to run after one iteration till that join is complete, ie.
        # the thread which is started as p1 is completed, so it essentially becomes a serial work instead of 
        # parallel
    for i in range(len(searchList)):
        p[i].join()
        print("#" + str(i) + " joined")
    while q.empty() is not True:
        qcount = qcount+1
        queue_top = q.get()
        sourceList.append(queue_top[0])
        searchTerms.append(queue_top[1])
        dates.append(queue_top[2])
        products.append(queue_top[3])
        prices.append(queue_top[4])
        regularPrices.append(queue_top[5])
        onSales.append(queue_top[6])
        amazonChoices.append(queue_top[7])
        sponsoredList.append(queue_top[8])
        positions.append(queue_top[9])
        pages.append(queue_top[10])
        brands.append(queue_top[11])
        MFGs.append(queue_top[12])
        variants.append(queue_top[13])
        packTypes.append(queue_top[14])
        packCounts.append(queue_top[15])
        packSizes.append(queue_top[16])
        print("Q Count " + str(qcount) + " pulled")
                
    #Only run once everything is done        
    print("total time taken: ", str(time.time()-startTime), " qcount: ", qcount)
    #print(q.get())
    

    #df = pd.DataFrame({'Source':sourceList, 'Keyword':searchTerms,'Date':dates, 'Product Name':products, 'Price':prices, 
    #    'Regular Price':regularPrices, 'On Sale':onSales, 'Amazon Choice':amazonChoices, 'Sponsored':sponsoredList, 'List Position':positions, 'Page':pages})
    #print(df)
    #df.to_csv('./amazon_data/' + str(date.today()) + '-SearchList.csv', index=False, encoding='utf-8')
    #print("Dataframe saved")


    tagged_df = pd.DataFrame({'Source':sourceList, 'Keyword':searchTerms,'Date':dates, 'Product_Name':products, 'Price':prices, 
        'Regular_Price':regularPrices, 'On_Sale':onSales, 'Featured':amazonChoices, 'Sponsored':sponsoredList, 
        'List_Position':positions, 'Page':pages, 'Brand':brands, 'MFG':MFGs, 'Variant':variants, 'Pack_Type':packTypes,
        'Pack_Count':packCounts, 'Pack_Size':packSizes})
    #tagged_df.to_csv('./amazon_data/' + str(date.today()) + '-SearchList-Tagged.csv', index=False, encoding='utf-8')
    #print("Tagged DataFrame Saved")

    #Now write to the database and overwrite if we already have Amazon scrape data from today
    numWritten = update_scrape_db(tagged_df, True)
    print("Entries written to DB: ", numWritten)

    #Send completion email so we can make sure data got recorded
    recipient = 'david@4sightassociates.com'
    subject = 'Daily Web Scrape Update'
    message = ("Web scraping finished with " + str(qcount) + " entries recorded. \n" +
        "Brands Tagged: " + str(len(brands) - brands.count('')) + "\n" +                  
        "MFGs Tagged: " + str(len(MFGs) - MFGs.count('')) + "\n" +                  
        "Variants Tagged: " + str(len(variants) - variants.count('')) + "\n" +                  
        "Pack Types Tagged: " + str(len(packTypes) - packTypes.count('')) + "\n" +                  
        "Pack Counts Tagged: " + str(len(packCounts) - packCounts.count('')) + "\n" +                  
        "Pack Sizes Tagged: " + str(len(packSizes) - packSizes.count(''))
        )                   

    send_email(recipient, subject, message)
    print("Message sent")

    #And finally, update the proxy list with tne new list
    #updateProxyFile('./proxyList.csv', proxyList)