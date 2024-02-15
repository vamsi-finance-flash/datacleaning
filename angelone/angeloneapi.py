from datetime import datetime
import time
import calendar
from SmartApi import SmartConnect #or from SmartApi.smartConnect import SmartConnect
import pyotp
import pandas as pd

api_key = 'bQHPsBqS'
username = 'I54718321'
pwd = '5492'
smartApi = SmartConnect(api_key)

token = "L5C5JUY5QGGDO6X4XIXL5334M4"
totp = pyotp.TOTP(token).now()

correlation_id = "abcde"
data = smartApi.generateSession(username, pwd, totp)
writer = pd.ExcelWriter('BroadBasedIndex.xlsx')

commandlog_path = "commandlog.txt"
fhand = open(commandlog_path,"a")

def pr(p):
    print(p)
    fhand.write(p+"\n")
# create an instance of the API class

def callthis(name,scripval,interval='ONE_MINUTE'):
    pr(f'Downloading for stock: {name}')
    new_order = ['Stock','Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    dy = pd.DataFrame(columns = new_order)
    dy.to_csv(f"{name}.csv",index = False)

    dx = pd.DataFrame(columns = ['Date','Minutes'])
    dx.to_csv(f"{name} log.csv",index = False)

    def working_function(from_date,to_date):
        re = {
                "exchange": "NSE",
                "symboltoken": scripval,
                "interval": interval,
                "fromdate": from_date + " 09:15",
                "todate":  to_date + " 15:30"
                }
        # Historical candel data
        dat = smartApi.getCandleData(re)
        # print(df)
        candledata = dat['data']
        if candledata is None:
            pr(f"No data at {from_date}, {to_date}")
        else:
            df = pd.DataFrame(candledata, columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'])

            # Convert DateTime column to datetime format
            df['DateTime'] = pd.to_datetime(df['DateTime'])

            # Create separate Date and Time columns
            df['Date'] = df['DateTime'].dt.date
            df['Time'] = df['DateTime'].dt.time

            # Drop the original DateTime column
            df = df.drop('DateTime', axis=1)

            # Add the Stock column
            df.insert(0, 'Stock', name)
            new_order = ['Stock','Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']

            # Reorganize the columns
            df = df[new_order]

            # Write DataFrame to CSV
            df.to_csv(f'{name}.csv', mode='a', header=False, index=False)
            pr(f"downloaded data at {from_date}, {to_date}")
            
            date_counts = df['Date'].value_counts()
            date_counts = sorted(date_counts.items(), key = lambda x:x[0])
            v = pd.DataFrame(date_counts,columns = ["Date","Minutes"])
            v.to_csv(f"{name} log.csv",mode='a',index=False,header=False)
    # Initialize the start and end years
    start_year = 2019
    end_year = 2024

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
            
            working_function(first_date,last_date)
            if year == end_year and month == 1:
                break
            time.sleep(0.5)



aodat = { 'NIFTY100 EQL WGT': 99926057, 'NIFTY MIDCAP 150': 99926060, 'NIFTY SMLCAP 50': 99926061, 
         'NIFTY200 QUALTY30': 99926064, 'NIFTY 100': 99926012, 'NIFTY MIDSML 400': 99926063, 
         'NIFTY 200': 99926033, 'NIFTY NEXT 50': 99926013, 'NIFTY SMLCAP 100': 99926032, 
         'NIFTY MIDCAP 100': 99926011, 'NIFTY 500': 99926004,"Nifty 50":99926000,"NIFTY MID SELECT":99926074}
#  'NIFTY MIDCAP 50': 99926014,
# 'NIFTY SMLCAP 250': 99926062, 'NIFTY50 EQL WGT': 99926056,
# callthis('NIFTY MIDCAP 100', str(99926011))
for name,scripval in aodat.items():
    callthis(name,str(scripval))
    pr("\n\n")
