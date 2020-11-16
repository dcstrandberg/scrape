#import time
#TODO: Add some form of analysis?, 
#Add whether an item is sponsored/featured -- Amazon's Choice, maybe others?
#Test out using sentiment analysis to tag the data -- brand name / pack size / pack count / $ per oz
#Add more sources: instacart, walmart, etc. Then create wrapper program that calls each of these subroutines

#Added regular price, on sale boolean, Amazon Choice Boolean and item position on the page
#Added more robust proxy cycling -- remove bad proxies from list, and re-scrape if we get too small a list

from proxy_list_scrape import scrapeProxyList
from datetime import date
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
positions=[] #List of where the product was positioned in the search
pages=[] #List to store ratings of the product

#Declare the request variables that determine how many requests will get made -- eventually these will be fed as arguments to the request function from a wrapper function
no_pages = 4 #20
keywords = ['soda','cereal', 'bars', 'drinks', 'water', 'coke']


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
            if len(proxies) < 10: proxies = scrapeProxyList()

            print("There was a connection error")
            proxyCounter += 1
        else:
            print("Now things are ok")
            noError = True

    content = r.content
    soup = BeautifulSoup(content, features="lxml")
    #print(soup.encode('utf-8')) # uncomment this in case there is some non UTF-8 character in the content and
                                 # you get error
	
    positionCounter = 0

    for d in soup.findAll('div', attrs={'class':'sg-col-4-of-24 sg-col-4-of-12 sg-col-4-of-36 s-result-item s-asin sg-col-4-of-28 sg-col-4-of-16 sg-col sg-col-4-of-20 sg-col-4-of-32'}):
        name = d.find('span', attrs={'class':'a-size-base-plus a-color-base a-text-normal'})
        price = d.find('span', attrs={'class':'a-offscreen'})
        
        #I may not be able to call contents immediately within the declaration
        regularPrice = d.find('span', attrs={'data-a-strike':'true'})

        amazonChoice = d.find('span', attrs={'class':'a-badge'})

        all=[]
		
        all.append(keyword)
        all.append(date.today())

        if name is not None:
            all.append(name.text)
        else:
            all.append("unknown-product")
 
        if price is not None:
            all.append(price.text)
        else:
            all.append('$0')

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

        all.append(str(positionCounter))
        #Now increment positionCounter so the next appended value is given the correct position
        positionCounter += 1

        all.append(str(pageNo))

        #if rating is not None:
            #all.append(rating.text)
        #else:
            #all.append('-1')
        q.put(all)
        #print("---------------------------------------------------------------") 
results = []
if __name__ == "__main__":
    m = Manager()
    q = m.Queue() # use this manager Queue instead of multiprocessing Queue as that causes error
    
    


    #Adding a for loop to cycle through the keywords -- not sure if a queue can work inside a for loop
    for word in keywords:
        p = {}    
        if sys.argv[1] in ['t', 'p']: # user decides which method to invoke: thread, process or pool
            for i in range(1,no_pages):
                if sys.argv[1] in ['t']:
                    print("starting thread: ",proxyCounter)
                    p[i] = threading.Thread(target=get_data, args=(word, i,q))
                    p[i].start()

                    #increment ProxyCounter
                    proxyCounter += 1

                elif sys.argv[1] in ['p']:
                    print("starting process: ",proxyCounter)
                    p[i] = Process(target=get_data, args=(word, i,q))
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
            positions.append(queue_top[7])
            pages.append(queue_top[8])

    #Only run once everything is done        
    print("total time taken: ", str(time.time()-startTime), " qcount: ", qcount)
    #print(q.get())
    df = pd.DataFrame({'Keyword':searchTerms,'Date':dates, 'Product Name':products, 'Price':prices, 
        'Regular Price:':regularPrices, 'On Sale?':onSales, 'Amazon Choice':amazonChoices, 'List Position':positions, 'Page':pages})
    #print(df)
    df.to_csv('./amazon_data/' + str(date.today()) + '-SearchList.csv', index=False, encoding='utf-8')
