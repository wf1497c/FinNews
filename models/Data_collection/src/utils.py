import pandas as pd
import yfinance as yf
import os

def get_prices(symbol, period):
    '''
    get price history of single stock
    symbol: int. Symbol of the stock to collect, e.g. MSFT for Microsoft
    period: str. Indicate period of collected data
    return: DataFrame. Price history of the stock
    '''
    s = yf.Ticker(symbol)

    # collect price history within the period
    hist = s.history(period=period)

    # drop redundent columns and add Date column
    hist.drop(columns=["Dividends",	"Stock Splits"], inplace=True)
    hist["Date"] = hist.index.date
    hist["Symbol"] = symbol

    return hist


def save_all_prices(first=False):
    '''
    get dataset of price histories of all stocks
    first: bool. True of the dataset was first built. 
                 New built dataset should contained longer period
    return: None type. Save metadata of collected data to data folder
    '''
    stock_list_file = os.path.abspath("./references/stock_list.csv")
    stock_list = pd.read_csv(stock_list_file)

    # if the dataset was not built yet, then collect 3 month data for initialization
    period = "1d" if not first else "3mo"

    # Dataframe recording all price histories
    hists = pd.DataFrame([])

    if first:
        # build valid_stock from scratch
        valid_stock = []
    else:
        # just read the list
        with open(path, "r") as f:
            valid_stocks = f.readline().split(" ")
            
    # iterate through all stocks recorded in yfinance
    if first:
        for symbol in stock_list["Symbol"]:
            try:
                hist = get_prices(symbol, period)
                hists = pd.concat([hists, hist], ignore_index=True)
                valid_stock.append(symbol)
            except Exception:
                pass
    else:
        for symbol in stock_list["Symbol"]:
            if symbol in valid_stock:
                hist = get_prices(symbol, period)
                hists = pd.concat([hists, hist], ignore_index=True)
    
    # save metadata
    metadata_file = "./data/raw/price_history.json"
    hists.to_json(metadata_file)

    # save valid stock
    if first:
        with open("references/valid_stock_list.txt", "w") as f:
            for s in valid_stock:
                f.write(s)

if __name__ == "__main__":
    save_all_prices(False)