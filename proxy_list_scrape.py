import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from multiprocessing import Process, Queue, Pool, Manager


def testProxies(aProxy, headers, q):
    #Declare All so we can use the queue
    all=[]
    
    if 'http' in aProxy: 
        printProxy = aProxy['http']
    else:
        printProxy = aProxy['https']

    try:
        #print("in the try")

        r = requests.get("https://www.amazon.com/s?k=cereal&page=1", headers=headers, proxies=aProxy, timeout=15)
            
    except:
        #print("in the except")

        #print("There was a connection error")
        print("Bad Proxy Check: " + printProxy)

    else:
        #print("in the else")

        content = r.content
        soup = BeautifulSoup(content, features="lxml")
        
        if len(soup.findAll('div', attrs={'data-index':re.compile(r'\d+')})) == 0:
            #print("There was a captcha error")
            print("Bad Proxy Check: " + printProxy)
        else:
            #If nothing is wrong with the proxy, add the proxy and put it in the queue
            all.append(aProxy)
            q.put(all)
    return


def scrapeProxyList(): 
    m = Manager()
    q = m.Queue() # use this manager Queue instead of multiprocessing Queue as that causes error
    qcount = 0
    
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    
    #Create a list of dictionaries to return for proxy purposes
    proxyList = []
    workingProxyList = []

    r = requests.get("https://www.us-proxy.org/", headers=headers)
    content = r.content
    soup = BeautifulSoup(content, features="lxml")
    #with open('proxy_scrape.html', 'w', encoding='utf-8') as outfile:
            #outfile.write(str(soup))
    #print(soup.encode('utf-8')) # uncomment this in case there is some non UTF-8 character in the content and
                                 # you get error
	
    for d in soup.findAll('tr'):
        
        address = d.contents[0]
        port = d.contents[1]
        https = d.contents[6]
        

        if address.text != "IP Address" and https is not None and address is not None and port is not None:
            if https.text == "yes":

                proxyList.append( {'http': 'http://' + address.text + ":" + port.text} )
            
            elif https.text == "no":

                proxyList.append( {'https': 'https://' + address.text + ":" + port.text} )
                
    ###TESTING THE PROXIES IS SOMETHING THAT SHOULD BE DONE MULTIPROCCESSED, SO WE'RE DOING THIS SHIT....
    p = {}    

    for i in range(len(proxyList)):            
        print("starting proxy process: ", i)
        p[i] = Process(target=testProxies, args=(proxyList[i], headers, q))
        p[i].start()

               # join should be done in seperate for loop 
        # reason being that once we join within previous for loop, join for p1 will start working
        # and hence will not allow the code to run after one iteration till that join is complete, ie.
        # the thread which is started as p1 is completed, so it essentially becomes a serial work instead of 
        # parallel
    for i in range(len(proxyList)):
        p[i].join()
        print("Proxy " + str(i) + " joined")
    
    while q.empty() is not True:
        qcount = qcount+1
        queue_top = q.get()
        workingProxyList.append(queue_top[0])
        print("Proxy Q " + str(qcount) + " pulled")
                
    #Only run once everything is done        
    #print("proxy qcount: ", qcount)
    
    #return proxyList
    return workingProxyList

#This function should only ever need to be run once...
def createProxyFile(filePath):

    #scrape the proxyList
    proxyList = scrapeProxyList()
    keys = []
    values = []

    for aProxy in proxyList:
        temp = aProxy.popitem()
        keys.append(temp[0])
        values.append(temp[1])

    df = pd.DataFrame({'http':keys, 'Proxy':values})
    df.to_csv(filePath, index=False, encoding='utf-8')


#This function is what we'll use to update the csv file
def updateProxyFile(filePath, proxyList):

    keys = []
    values = []

    for aProxy in proxyList:
        temp = aProxy.keys()
        keys.append(temp[0])

        temp = aProxy.values()
        values.append(temp[0])

    df = pd.DataFrame({'http':keys, 'Proxy':values})
    df.to_csv(filePath, index=False, encoding='utf-8')


#And this function is what we'll use to read in the proxy List and update it
def getProxyList(filePath):
    
    df = pd.read_csv(filePath)
    newArray = []
    #newArray.append({df['http'], df['Proxy']})
    
    for i in range(len(df['http'])):

        newArray.append( {df['http'][i]: df['Proxy'][i]} )

    return newArray

#Comment this out after testing ;)
#print(scrapeProxyList())
#createProxyFile("./proxyList.csv")