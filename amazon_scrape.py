#import time
#TODO: Add some form of analysis?, 
#Add reading in the search terms from a file or something, so it's easily editable?
#Add best seller flag?
#Add check on whether there was ANY data pulled or if it appears to be a captcha page
    #It should try a new proxy, and if it tries... 100 proxies or something, then it should error and send a message to an email that there's an issue
#Test out using sentiment analysis to tag the data -- brand name / pack size / pack count / $ per oz
#Add more sources: instacart, walmart, etc. Then create wrapper program that calls each of these subroutines

from proxy_list_scrape import scrapeProxyList
from datetime import date
import re
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from multiprocessing import Process, Queue, Pool, Manager
import threading
import sys

#Declare the variables that will be needed to run the request loop
proxies = scrapeProxyList() 
proxyCounter = 0
startTime = time.time()
qcount = 0

#Declare the lists that will be used to store the final dataframe
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

#Declare the request variables that determine how many requests will get made -- eventually these will be fed as arguments to the request function from a wrapper function
no_pages = 4
keywords = ['Soda', 'Water', 'Sports Drinks', 'Coffee', 'Cereal', 'Snack Bars', 'Chips', 'Snacks', 'Contact Lenses', 'Coke', 'Fanta', 
   'Sprite', 'Powerade', 'Frosted Flakes', 'Special K', 'Froot Loops', 'Raisin Bran', 'Pringles', 'Cheez It', 'Rice Krispies', 'Rice Krispies Treats', 
   'Pop Tarts', 'Acuvue', 'Oasys', 'Pet Food', 'Dog Food', 'Cat Food']
#keywords = ['cereal']

#Re organizing things
# - putting proxy cycling into its own function
# - putting request in its own function, then the different soups search into their own function?
# - putting search term & page number into one list that can be cycled through so all the things can be threaded separately


def get_data(keyword, pageNo,q):  
    #use this to access the global proxyCounter variable
    global proxyCounter
    global proxies

    #wait for a random period of time before fetching the next page, to help avoid being blocked by amazon
    time.sleep(random.random()) 
    
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    
    #Keep trying until an exception isn't raised
    noError = False
    
    while not noError:
        try:
            r = requests.get("https://www.amazon.com/s?k=" + keyword + "&page=" + str(pageNo), headers=headers, proxies=proxies[proxyCounter % len(proxies)])
        except:
            #remove the bad proxy from the list so we don't try it again
            del proxies[proxyCounter % len(proxies)]

            #check to make sure we still have a sizeable list of proxies, and if it gets below 10 proxies, scrape a new list
            if len(proxies) < 5: proxies = scrapeProxyList()

            print("There was a connection error")
            proxyCounter += 1
        else:
            print("Now things are ok")
            #THIS IS ALL DEBUGGING NONSENSE
            if 'http' in proxies[proxyCounter % len(proxies)]: 
                printProxy = proxies[proxyCounter % len(proxies)]['http']
            else:
                printProxy = proxies[proxyCounter % len(proxies)]['https']

            print("This proxy worked " + printProxy)

            #Add here some checking for whether soup is readable

            content = r.content
            soup = BeautifulSoup(content, features="lxml")
            
            if len(soup.findAll('div', attrs={'data-index':re.compile(r'\d+')})) == 0:
                #remove the bad proxy from the list so we don't try it again
                del proxies[proxyCounter % len(proxies)]

                #check to make sure we still have a sizeable list of proxies, and if it gets below 10 proxies, scrape a new list
                if len(proxies) < 5: proxies = scrapeProxyList()

                print("There was a captcha error")
                proxyCounter += 1
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
                name = product.find('span', attrs={'class':'a-size-base-plus a-color-base a-text-normal'})
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

            
                q.put(all)

                #increment carousel counter -- shouldn't matter if this isn't a carousel
                carouselCounter += 1
             
    #DEBUGGING -- if this is page 1 of the first loop, just save the full html file
    if pageNo == 1 and keyword == 'cereal':
        with open('cereal_html.html', 'w', encoding='utf-8') as outfile:
            outfile.write(str(soup))


results = []
if __name__ == "__main__":
    m = Manager()
    q = m.Queue() # use this manager Queue instead of multiprocessing Queue as that causes error
    
    


    #Adding a for loop to cycle through the keywords -- not sure if a queue can work inside a for loop
    for word in keywords:
        p = {}    

        for i in range(1,no_pages):            
            print("starting process: ",proxyCounter)
            p[i] = Process(target=get_data, args=(word, i, q))
            p[i].start()

            #increment ProxyCounter
            proxyCounter += 1

            # join should be done in seperate for loop 
            # reason being that once we join within previous for loop, join for p1 will start working
            # and hence will not allow the code to run after one iteration till that join is complete, ie.
            # the thread which is started as p1 is completed, so it essentially becomes a serial work instead of 
            # parallel
        for i in range(1,no_pages):
            p[i].join()
        while q.empty() is not True:
            qcount = qcount+1
            queue_top = q.get()
            searchTerms.append(queue_top[0])
            dates.append(queue_top[1])
            products.append(queue_top[2])
            prices.append(queue_top[3])
            regularPrices.append(queue_top[4])
            onSales.append(queue_top[5])
            amazonChoices.append(queue_top[6])
            sponsoredList.append(queue_top[7])
            positions.append(queue_top[8])
            pages.append(queue_top[9])

    #Only run once everything is done        
    print("total time taken: ", str(time.time()-startTime), " qcount: ", qcount)
    #print(q.get())
    df = pd.DataFrame({'Keyword':searchTerms,'Date':dates, 'Product Name':products, 'Price':prices, 
        'Regular Price:':regularPrices, 'On Sale?':onSales, 'Amazon Choice':amazonChoices, 'Sponsored':sponsoredList, 'List Position':positions, 'Page':pages})
    #print(df)
    df.to_csv('./amazon_data/' + str(date.today()) + '-SearchList.csv', index=False, encoding='utf-8')
