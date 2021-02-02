#Functions to read/write/create the DB
#TODOS: Add Functions for:
###Writing to database after scrape
###Checking whether a date is already in the database and delete it if necessary

import pyodbc
import pandas as pd
import sqlite3
from sqlite3 import Error
from pathlib import Path

#def connect_to_db():
#    try: 
#        conn = pyodbc.connect('Driver={SQL Server};'
#        'Server=FALL15\SQLEXPRESS;'
#        'Port=3882;'
#        'User=FALL15\david;'
#        'Database=Ecommerce;'
#        'Trusted_Connection=yes;')
#    except: 
#        print("Error in connection")
#    
#    return conn


def connect_to_db(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except:
        print("Could not connect to DB")

    return connection

def execute_query(connection, query, errorMsg = "The error occurred"):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except:
        print(errorMsg)
    return cursor

def pull_all_data():
    #connect to the DB
    conn = connect_to_db('sqlite/ecommerce.db')
    results = None 

    try:
        #cursor.execute('SELECT * FROM scrape_data')
        #conn.commit()
        results = pd.read_sql_query('SELECT * FROM scrape_data', conn)
        print("Selected Data")
        print(len(results))
    except:
        print("Error with cursor")

    

    #results = cursor.fetchall()
    if results is not None:
        print(len(results))
    else:
        print("No values in table")
    #cursor.close()

    return results

def update_scrape_db(df, replace = False):
    #connect to the DB
    conn = connect_to_db('sqlite/ecommerce.db')

    #Get a list of the dates in the dataframe
    dateList = []
    #for aDate in df['Date']:
    #    if aDate not in dateList:
    #        dateList.append(aDate)
    
    for i in range(len(list(df['Date']))):
        if (df['Source'][i], df['Date'][i]) not in dateList:
            dateList.append( (df['Source'][i], df['Date'][i]) )


    #print("Dates in DF: " + str(dateList))
    
    #Now check if either of those dates are in the database
    for aDate in dateList:
        
        result = execute_query(conn, "SELECT COUNT(Date) FROM scrape_data WHERE Source='" + str(aDate[0]) + "' AND Date='" + str(aDate[1]) + "'", "Error in Before Count")
        temp = result.fetchone()
        #if temp is not None: print("Before count: " + str(temp[0]))
        
        #If replace is true, delete the current values w/ those dates
        if replace:
            execute_query(conn, "DELETE FROM scrape_data WHERE Source='" + str(aDate[0]) + "' AND Date='" + str(aDate[1]) + "'", "Error in Deletion")
            result = execute_query(conn, "SELECT COUNT(Date) FROM scrape_data WHERE Source='" + str(aDate[0]) + "' AND Date='" + str(aDate[1]) + "'", "Error in PostDelete Count")
            temp = result.fetchone()
            #if temp is not None: print("After Delete count: " + str(temp[0]))

    #Use the DF and the connector to update the table
    df.to_sql('scrape_data', conn, index=False, if_exists='append')
        
    #Now check to see how things are looking on the date front
    for aDate in dateList:
        
        result = execute_query(conn, "SELECT COUNT(Date) FROM scrape_data WHERE Source='" + str(aDate[0]) + "' AND Date='" + str(aDate[1]) + "'", "Error in New Entry Count")
        temp = result.fetchone()
        #if temp is not None: print("New Entry count: " + str(temp[0]))

    conn.close()
    return str(temp)

def append_csv_to_db(path, sourceName, replaceDupeDates = False):
    #Create the dataframe from the CSV, append the Source column
    df = pd.read_csv(path)
    sourceList = [sourceName] * len(df['Keyword'])

    new_df = pd.DataFrame({'Source':sourceList, 'Keyword':df['Keyword'],'Date':df['Date'], 'Product_Name':df['Product Name'], 'Price':df['Price'], 
            'Regular_Price':df['Regular Price:'], 'On_Sale':df['On Sale'], 'Featured':df['Amazon Choice'], 'Sponsored':df['Sponsored'], 
            'List_Position':df['List Position'], 'Page':df['Page'], 'Brand':df['Brand'], 'MFG':df['MFG'], 'Variant':df['Variant'], 'Pack_Type':df['Pack Type'],
            'Pack_Count':df['Pack Count'], 'Pack_Size':df['Pack Size']})

    #Finally call update_scrape_list to update the db
    update_scrape_db(new_df, replaceDupeDates)
    
            

if __name__ == "__main__":

    #OK now for the first time this file is run -- converting all the data in the csv files into the db
    conn = connect_to_db('./sqlite/ecommerce.db')
    #execute_query(conn, "CREATE TABLE scrape_data (Source text, Keyword text, Date text, Product_Name text, Price text, Regular_Price text, On_Sale text, Featured text, Sponsored text, List_Position text, Page text, Brand text, MFG text, Variant text, Pack_Type text, Pack_Count text, Pack_Size text);", "Error creating table")

    #csvDirectory = Path('./amazon_data/')
    #csvPathList = csvDirectory.iterdir()

    #for csv in csvPathList:
    #    print(str(csv))
    #    if "tagged2" in str(csv).lower():
    #        append_csv_to_db(csv, "Amazon", True)
    #        print("Inserted " + str(csv) + " into DB")

    #append_csv_to_db('./amazon_data/2020-12-05-SearchList-Tagged.csv', "Amazon", True)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(Source) FROM scrape_data')
        conn.commit()
        print("Selected Data")
    except:
        print("Error with cursor 2")

    results = cursor.fetchone()
    if results is not None:
        print("Selected " + str(results))
    else:
        print("No values in table")


    try:
        cursor.execute('SELECT * FROM scrape_data')
        conn.commit()
        print("Selected Data")
    except:
        print("Error with cursor 2")

    
    for i in range(10):
        results = cursor.fetchone()
        if results is not None:
            print(str(results))
        else:
            print("No values in table")


    cursor.close()