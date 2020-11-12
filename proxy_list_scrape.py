import requests
from bs4 import BeautifulSoup

def scrapeProxyList(): 
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    
    #Create a list of dictionaries to return for proxy purposes
    proxyList = []

    r = requests.get("https://www.us-proxy.org/", headers=headers)
    content = r.content
    soup = BeautifulSoup(content, features="lxml")
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
                
    
    return proxyList

#Comment this out after testing ;)
#print(scrapeProxyList())