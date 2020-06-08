from yahoofinancials import YahooFinancials
import pandas as pd
import datetime as dt
from arctic import Arctic, TICK_STORE
import warnings


#Step 1: Connect to ARCTIC - Note: Change connection string as needed
uri = "mongodb://--------------.us-east-2.compute.amazonaws.com:27017/arctic?authSource=arctic"
warnings.filterwarnings("ignore")

found = []
notfound =[]
already =[]

store = Arctic(uri)
ARCTIC_NAME='Russell'
store.initialize_library(ARCTIC_NAME, lib_type=TICK_STORE)
library = store[ARCTIC_NAME]

#pull from an excel spreadsheet
source = "/Users/saffy/Downloads/IWV_holdings.xlsx"
StockFromExcel =pd.read_excel(source)
StockFromExcelList = StockFromExcel['Ticker'].values.tolist()

# extracting stock data (historical close price) for the stocks identified
# start a dictionary data frame
ohlc_dict = {}

# todays date in decimal form
end_date = (dt.date.today()).strftime('%Y-%m-%d')
# todays date in decimal form --- subtract timedelta to get beginning of day
beg_date = (dt.date.today() - dt.timedelta(3650)).strftime('%Y-%m-%d')

#the list of symbols in arctic
symbols = library.list_symbols()

for ticker in StockFromExcelList:
    # if data is already in the arctic
    if ticker in symbols:
        already.append(ticker)
        print(str(ticker) + " is already in database")
        continue
    # if data is not in arctic and needs to be uploaded
    else:
        try:
            print(ticker)
            yahoo_financials = YahooFinancials(ticker)
            # get daily data for each ticker
            json_obj = yahoo_financials.get_historical_price_data(beg_date, end_date, "daily")
            # price for the ticker
            ohlc = json_obj[ticker]['prices']
            # Adj close, open, low, high, and volume for each day of the ticker
            temp = pd.DataFrame(ohlc)[["formatted_date","adjclose","open","low","high","volume"]]
            temp.set_index("formatted_date",inplace=True)
            temp.dropna(inplace=True)
            # put data into a csv format
            temp.to_csv('BANKING_STOCK.csv')
            # place data into a dataframe
            df = pd.read_csv('BANKING_STOCK.csv')
            # get date and time to store into arctic
            df['formatted_date'] = pd.to_datetime(df['formatted_date'])
            df = df.rename(columns={'formatted_date': 'Datetime'})
            df['Datetime'] = pd.to_datetime(df['Datetime'], unit='ns', utc=True)
            df = df.rename(columns={'adjclose': 'close'})
            df.set_index('Datetime', inplace=True)
            print(df.head())
            #put the data into arctic
            library.write(ticker,df)
        # if data is not found in Yahoo Finance
        except:
            notfound.append(ticker)
            print(str(ticker) + "m Data not found in YahooFinance")