import os
import datetime
import numpy as np
import time
import pandas as pd
import datapackage
import ssl

#Start and End dates
TODAY = datetime.date.today()
START = datetime.datetime(2021, 3, 11)

#HTTPS Security bypass
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context


def getbasedf(): 
    data = pd.read_csv('SnP_Data_Bloomberg.csv')

    data.loc[64, 'Ticker'] = 'BRK-B'
    data.loc[80, 'Ticker'] = 'BF-B'
    
    data['12 Months Close'] = np.nan
    data['9 Months Close'] = np.nan
    data['6 Months Close'] = np.nan
    data['3 Months Close'] = np.nan
    data['Today Close'] = np.nan
    data['Price Source'] = np.nan
    return data

def dt_convert(date):
    #Converts a date from legible english to the format that Yahoo uses
    return int(time.mktime(date.timetuple()))

def getTickerHistoricalPrices(ticker):
    #Gets price history for a ticker
    query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={dt_convert(START)}&period2={dt_convert(TODAY)}&interval=1d&events=history&includeAdjustedClose=true'
    df = pd.read_csv(query_string)
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.reindex(index=df.index[::-1])
    df = df.reset_index()
    price_tuple = (df.loc[0, 'Adj Close'], df.loc[63+1, 'Adj Close'], df.loc[2*63+1, 'Adj Close'], df.loc[3*63+1, 'Adj Close'], df.loc[252, 'Adj Close'], query_string)
    return price_tuple

#Main
#Gets downloaded dataset
df = getbasedf()

# # # # payload = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
# # # # df = payload[0]
# # # # print(df)
# # # # df3 = pd.DataFrame()
# # # # df3 = df['Symbol']
# # # # df3.to_csv('SnP_tickers.csv')
# # # # exit()

#Iterrates over all tickers and gets live price data
for index, row in df.iterrows():
    time.sleep(1)
    if index < 470:
        continue
    elif index == 358:
        continue
    else:
        ticker = row['Ticker']
        print(index, ticker)
        prices = getTickerHistoricalPrices(ticker)
        df.loc[index, 'Today Close'] = prices[0]
        df.loc[index, '3 Months Close'] = prices[1]
        df.loc[index, '6 Months Close'] = prices[2]
        df.loc[index, '9 Months Close'] = prices[3]
        df.loc[index, '12 Months Close'] = prices[4]
        df.loc[index, 'Price Source'] = prices[5]

        if index % 50 == 0:
            print('Saving Progress')
            df.to_csv('Live_{}.csv'.format(index))

df['3m change'] = df['Today Close']/df['3 Months Close'] - 1
df['6m change'] = df['Today Close']/df['6 Months Close'] - 1
df['9m change'] = df['Today Close']/df['9 Months Close'] - 1
df['12m change'] = df['Today Close']/df['12 Months Close'] - 1

df.to_csv('Live.csv')


