'''
Author: Louis Tang (wf1497c on github)
Purpose: Daily ETL job for FinTalks App
Descriptions:
    The following job will be executed in midnight(in EST) on weekdays to scrape data
    1. Extract: scrape daily prices data of most active shares in US stock market and related tweets from Twitter
                via yahoo-finance and snscrape API
    2. Transform: Collected data was first organzied in pandas dataframe and transformed, e.g. chose cols, roughly 
                  filtered spam tweets, transformed col type like datetime to string, and transformed into JSON-likes format
    3. Load: Upload transformed data from previous step to MongoDB, which is document-based storage system 
'''
import pandas as pd
import yfinance as yf
import os
import pytz
from datetime import datetime, timedelta
import snscrape.modules.twitter as sntwitter
import pymongo

def get_prices(ticker, period):
    '''
    get price history of single stock
    symbol: int. Symbol of the stock to collect, e.g. MSFT for Microsoft
    period: str. Indicate period of collected data
    return: DataFrame. Price history of the stock
    '''
    # collect price history within the period
    s = yf.Ticker(ticker)
    hist = s.history(period=period)

    # drop redundent columns and add Date column
    try:
        hist.drop(columns=["Dividends",	"Stock Splits"], inplace=True)
        hist["Date"] = str(hist.index.date[0].year) + "-" + str(hist.index.date[0].month) + "-" + str(hist.index.date[0].day)
        hist["Ticker"] = ticker
        cols = ["Date", "Ticker", "Open", "High", "Low", "Close"]
        hist = hist[cols]
        hist.index = [0]
    except:
        print("Error: Input ticker is invalid!")

    return list(hist.T.to_dict().values()) # transform into MongoDB compatible format

def update_price_dataset():
    '''
    get new data of price histories of all stocks
    return: None. Upload to database
    '''
    # MongoDB personal info
    client = pymongo.MongoClient("mongodb+srv://wf1497c:a27755267@fintalk.sugc41p.mongodb.net/?retryWrites=true&w=majority")
    db = client['App'] # Database
    collection = db["Price"] # Collection: students (similar to table in RDBMS)

    # Collect data by 1 day
    period = "1d"

    # read the list of valid and target tickers
    path = "./references/target_symbols.txt"
    with open(path, "r") as f:
        target_symbols_list = f.readlines()
    target_symbols_list = [s.replace("\n","") for s in target_symbols_list]
    target_symbols_list = [v.split(",")[0] for v in target_symbols_list]

    # Collect Data
    for ticker in target_symbols_list:
        # get price data within one day using ticker 
        price = get_prices(ticker, period)

        # Upload to MongoDB
        try:
            collection.insert_many(price)
        except Exception as e:
            print(e)


def get_tweets(ticker):
    '''
    Scrape tweets from Twitter using snscrape api
    ticker: str. Date of the ticker to collect
    return: List. Contain related information of tweets such as id or text
    '''
    tweet_limit = 300 # Num of collected Tweets

    # Specify timezone 
    tz = pytz.timezone('America/New_York')

    until = datetime.now(tz)
    year, month, day = until.year, until.month, until.day

    since = until - timedelta(days=1)
    s_year, s_month, s_day = since.year, since.month, since.day

    # To prevent spam messages posted by the same person, use a set to record users so far
    users = set()

    # Spam detector: simply not use tweets with specific keywords
    spams = set([
                    "for next week",
                    "Top analyst",
                    "LEVELS POSTED",
                    "Buying the Dip Right Now"
                ])

    # Creating list to append tweet data to
    tweets_list = []

    # Using TwitterSearchScraper to scrape data and append tweets to list
    count = 0
    query = f"${ticker} lang:en since:{s_year}-{s_month}-{s_day} until:{year}-{month}-{day}"
    search_res = sntwitter.TwitterSearchScraper(query).get_items()

    for _,tweet in enumerate(search_res):
        # Prevent advertisement with more than one cashtags
        # e.g. "Live day trading: $SAVE $WORK $T $AAPL $C $MSFT $SPY"
        if not tweet.cashtags or len(tweet.cashtags) > 3:
            continue

        # Prevent spam messages like: "top analyst with discord link: an url" 
        spam = False
        if tweet.content:
            for s in spams:
                if s in tweet.content:
                    spam = True
        if spam:
            continue
        
        # Prevent tweets from the same user
        if tweet.user.id not in users:
            count += 1
            users.add(tweet.user.id)
        else:
            continue
        
        # # of collected tweets
        if count > tweet_limit:
            break
        
        tweets_list.append([tweet.id,
                            tweet.user.id,
                            tweet.date, 
                            tweet.content, 
                            tweet.likeCount, 
                            tweet.replyCount,
                            tweet.retweetCount,
                            tweet.hashtags,
                            ticker,
                            tweet.user.location,
                            tweet.source.split(">")[1].split("<")[0]
                            ])
        
    # Creating a dataframe from the tweets list above
    tweets_df = pd.DataFrame(tweets_list, columns=["Id",
                                                    "User",
                                                    'Datetime', 
                                                    'Text', 
                                                    'Like_count', 
                                                    "Reply_count",
                                                    "Retweet_count", 
                                                    "Hashtags",
                                                    "Ticker",
                                                    "User_location",
                                                    "User_source"])

    return list(tweets_df.T.to_dict().values()) # transform into MongoDB compatible format

def update_tweets_dataset():
    '''
    scrape Twitter data and upload all of them to MongoDB
    return: None. Upload to database
    '''
    # MongoDB personal info
    client_info = "mongodb+srv://wf1497c:a27755267@fintalk.sugc41p.mongodb.net/?retryWrites=true&w=majority"
    client = pymongo.MongoClient(client_info)
    db = client['App'] # Database
    collection = db["Twitter"] # Collection: students (similar to table in RDBMS)
    
    tweet_lower_limit = 15 # Lower bound of nums

    # Get ticker lists
    path = "./references/target_symbols.txt"
    with open(path, "r") as f:
        target_symbols_list = f.readlines()
    target_symbols_list = [v.replace("\n","") for v in target_symbols_list]
    target_symbols_list = [v.split(",")[0] for v in target_symbols_list]

    social_valid_target_symbols = []
    # Collect data 
    for ticker in target_symbols_list:
        # get tweets using ticker
        tweets_df = get_tweets(ticker)

        # Upload to MongoDB
        try:
            if len(tweets_df) > tweet_lower_limit:
                collection.insert_many(tweets_df)
                print(f"{len(tweets_df)} {ticker} tweets were collected and uploaded to MongoDB!")
                social_valid_target_symbols.append(ticker)
        except Exception as e:
            print(e)

    return social_valid_target_symbols

def get_daily_data():
    '''
    Get daily data, including stock prices and social media responses
    return: None. Upload data to MongoDB 
    '''
    # print("-------------- Start collecting stock prices -------------- ")
    # update_price_dataset()
    # print("-------------- Done --------------")

    print("-------------- Start collecting tweets -------------- ")
    social_valid_target_symbols = update_tweets_dataset()

    with open("social_valid_target_symbols.txt", "w") as f:
        for s in social_valid_target_symbols:
            f.write(s+"\n")

    print("-------------- Done --------------")

if __name__ == "__main__":
    get_daily_data()