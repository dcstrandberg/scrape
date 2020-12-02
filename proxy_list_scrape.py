import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def testProxies(proxyList, headers):
    #Loop through each proxy in the list and only share the valid proxies
    badProxyList = []
    
    for i in range( min( len(proxyList), 100) ):
        proxy = proxyList[i]
        
        #print("in the loop")

        if 'http' in proxy: 
            printProxy = proxy['http']
        else:
            printProxy = proxy['https']

        try:
            #print("in the try")

            r = requests.get("https://www.amazon.com/s?k=cereal&page=1", headers=headers, proxies=proxy, timeout=10)#5)
                
        except:
            #print("in the except")
            #remove the bad proxy from the list so we don't try it again
            badProxyList.append( proxy )
            proxyList.remove( proxy )
            #print("There was a connection error")
            print("Bad Proxy Check: " + printProxy)

        else:
            #print("in the else")

            content = r.content
            soup = BeautifulSoup(content, features="lxml")
            
            if len(soup.findAll('div', attrs={'data-index':re.compile(r'\d+')})) == 0:
                #remove the bad proxy from the list so we don't try it again
                badProxyList.append( proxy )
                proxyList.remove( proxy )

                #print("There was a captcha error")
                print("Bad Proxy Check: " + printProxy)
    
    print("Length of Final List: " + str(len(proxyList)))
    print("Length of Bad List: " + str(len(badProxyList)))

    for aProxy in badProxyList:
        if aProxy in proxyList: 
            print("Bad Proxy found in Good List")
            print(aProxy)

    return proxyList


def scrapeProxyList(): 
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    
    #Create a list of dictionaries to return for proxy purposes
    proxyList = []

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
                
    #Adding a part two, a second scrape to build out the list a bit more
    #r = requests.get("https://www.proxynova.com/proxy-server-list/country-us/", headers=headers)
    #content = r.content
    #soup = BeautifulSoup(content, features="lxml")
    #print(soup.encode('utf-8')) # uncomment this in case there is some non UTF-8 character in the content and
                                 # you get error
	
    #for d in soup.findAll('tr'):
    #    print(d)
    #    if d['data-proxy-id'] is None: continue
    #    
    #    td = d.contents[0]

    #    if td is not None and d['data-proxy-id'] is not None:
    #        abbr = td['abbr']
    #        
    #        #if abbr is not None:
    #        addrText = abbr['title']
    #        
    #    port = d.contents[1]
    #    #https = d.contents[6]
    #    

    #    if d.parent.name == "tbody"  and port is not None:

    #        portText = port.text

    #        #Quickly strip off any quotes around either:
    #        if addrText[0] == '"': addrText = addrText[1:]
    #        if addrText[-1] == '"': addrText = addrText[:-1]

    #        proxyList.append( {'http': 'http://' + addrText + ":" + portText} )

    #Before returning the list, test the proxies for usability
    #proxyList = testProxies(proxyList, headers)

    return proxyList

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