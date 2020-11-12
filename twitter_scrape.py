#This is meant to be first attempt at scraping twitter

#Let's first pull just a single tweet and save it in csv?
#import the twitter_scrape module

from twitter_scraper import get_tweets

#Open a file for saving the data
#outfile = open("./tweet_data.csv", "w")

#Scrape the data
for tweet in get_tweets('twitter', pages=1):
    
    print(tweet['text'])
    
    #outfile.write(tweet['time'] + ',')
    #outfile.write(tweet['text'] + ',')
    #outfile.write(tweet['likes'] + ',\n')


#close the file
#outfile.close()