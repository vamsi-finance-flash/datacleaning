import calendar
from datetime import datetime
import time
from py5paisa import FivePaisaClient
import pandas as pd
import pyotp
from data_needed import *

fhand = open("commandlogd.txt","a")

modh = open("modlog.txt","a")
fhand1 = open("edits.txt","a")


def pr(p):
    print(p)
    fhand.write(p+"\n")


def report(stock_name):
    df = pd.read_csv(f"niftyall/{stock_name}.csv")
    date_dict = df['Date'].value_counts()
    date_dict = sorted(date_dict.items(), key = lambda x:x[0])

    date_count = pd.DataFrame(date_dict,columns = ["Date","Minutes"])
    date_count.insert(0, 'Stock', stock_name)
    date_count.to_csv(f"niftyalllogs/{stock_name} log.csv",index=False)
    modh.write(f"{stock_name}---------------------------------------\n")
    df = df[df["Time"].isin(times)]
    for date,minutes in date_dict:
        if minutes < 300:
            df = df[df["Date"] != date]
            modh.write(f"Data at {date} is removed\n")
            fhand1.write(f"{stock_name},{date}\n")
        elif minutes>=300 and minutes<=376:
            ml = df[df["Date"] == date]
            flen = len(ml)
            ml.drop_duplicates()
            llen = len(ml)
            if flen != llen:
                fhand1.write(f"Data Duplicates ==== {date} \n")
                if llen<300:
                    df = df[df["Date"] != date]
                    modh.write(f"For {date} data is Dropped1\n")
                elif llen>=300:
                    df = pd.concat([df,ml])
                    modh.write(f"For {date} data is reshaped\n")
        elif minutes > 376:
            mod = df[df["Date"] == date]
            mod = mod.drop_duplicates()
            df = df[df["Date"] != date]
            if len(mod) > 300 and len(mod) <= 376:
                df = pd.concat([df,mod])
                modh.write(f"For {date} data is reshaped\n")
            else:
                modh.write(f"Data at {date} is dropped\n")
                fhand1.write(f"{stock_name},{date}\n")
    df.sort_values(by=['Date', 'Time'],inplace=True)
    df.to_csv(f"niftymod/{stock_name}.csv",index=False)
    modh.write("\n\n")
    print(stock_name)



def callthis(stock_name,scrip_code):

    pr(f'Downloading for stock: {stock_name}')
    new_order = ['Stock','Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    dy = pd.DataFrame(columns = new_order)

    dy.to_csv(f"niftyall/{stock_name}.csv",index = False)
    def function(s,e):
        df=client.historical_data('N','C',scrip_code,'1m',s,e)
        if df is None or df.empty :
            pr(f"No data at {s}, {e}")
        else:
            # Convert the 'Datetime' column to datetime format
            df['Datetime'] = pd.to_datetime(df['Datetime'])

            # Create new columns 'Date' and 'Time'
            df['Date'] = df['Datetime'].dt.date
            df['Time'] = df['Datetime'].dt.time

            # Now you can drop the 'Datetime' column if you wish
            df = df.drop(columns=['Datetime'])
            # Add a new column 'Stock' with the value 'SBIN'
            df = df.assign(Stock=stock_name)
            # Save the modified DataFrame back to a CSV file
            # Specify the new order of the columns
            new_order = ['Stock','Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']

            # Reorganize the columns
            df = df[new_order]

            df.to_csv(f'niftyall/{stock_name}.csv', mode='a', header=False, index=False)
            pr(f"Data saved {s}, {e}")

    # Initialize the start and end years
    start_year = 2019
    end_year = 2024

    # Initialize an empty list to store the dates
    dates = []

    # Loop over the years
    for year in range(start_year, end_year + 1):
        # Loop over the months
        for month in range(1, 13):
            # Get the last day of the month
            _, last_day = calendar.monthrange(year, month)
            
            # Get the first and last dates of the month
            first_date = datetime(year, month, 1)
            last_date = datetime(year, month, last_day)
            
            first_date = first_date.strftime("%Y-%m-%d")
            last_date = last_date.strftime("%Y-%m-%d")

            # Append the dates to the list
            function(first_date,last_date)
            time.sleep(0.1)
            if year == end_year and month == 1:
                break
    report(stock_name)



cred={
    "APP_NAME":"5P52108868",
    "APP_SOURCE":"21186",
    "USER_ID":"Hc0FH8u8rOL",
    "PASSWORD":"LLn1gb7AVSp",
    "USER_KEY":"IdjeAsV6ezoWweyyhKIZqTcmjkcfZhgj",
    "ENCRYPTION_KEY":"DbecoynDFWxpWlfQKWPvS4N4voOsSJCi"
    }

#This function will automatically take care of generating and sending access token for all your API's
TOTP = pyotp.TOTP('GUZDCMBYHA3DQXZVKBDUWRKZ').now()
client = FivePaisaClient(cred=cred)

# New TOTP based authentication
client.get_totp_session(52108868,TOTP,549255)

#scrip_dict is a file that has the data of stock name and stockcode in 5paisa
for k,v in scrip_dict.items():
    if k not in completed:
        callthis(k,v)
        time.sleep(1.5)
        pr("\n\n")
