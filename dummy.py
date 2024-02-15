import pandas as pd
import yfinance as yf
import time
from datetime import datetime

start_time = time.time()

fhand = open("editing.txt",'a')
fhand.write("\n\n")

fhand1 = open("modslog.txt",'a')
fhand1.write("\n\n")
new_order = ["Date","Open","Open_yf","Open_pc","High","High_yf", "High_pc", "Low","Low_yf","Low_pc","Close","Close_yf","Close_pc"]
def edit(stock_dat,yf_dat):
    yf_dat = yf_dat.rename(columns = {"Date":"Date_yf","Open":"Open_yf","Close":"Close_yf","High":"High_yf","Low":"Low_yf"})
    dz = pd.merge(stock_dat,yf_dat, left_on = "Date", right_on = 'Date_yf')
    for col in ["Open","High","Low","Close"]:
        dz[col+"_pc"] = abs(dz[col]-dz[col+"_yf"])*100/dz[col]
    dz = dz[new_order]
    return dz

def callthis(stock):
    stock_data = pd.read_csv(f"modifiednifty500/{stock}.csv")
    yf_data = pd.read_csv(f"yfin/{stock}.csv")

    ydd = list(yf_data["Date"])
    xl = len(stock_data)
    if xl<3:
        fhand1.write(f"{stock}, has very low values\n")
    else:
        stock_data["Date"] = pd.to_datetime(stock_data["Date"])
        stock_data = stock_data[stock_data["Date"].isin(ydd)]

        yf_data["Date"] = pd.to_datetime(yf_data["Date"])
        yf_data = yf_data[yf_data["Date"].isin(stock_data["Date"])]

        yf_data.drop("Volume",axis = 1)

        stock_daily = stock_data.groupby(stock_data['Date'].dt.date).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last'
        }).reset_index()

        stock_daily["Date"] = pd.to_datetime(stock_daily["Date"])
        org_len = len(stock_daily)
        mod_data = edit(stock_daily,yf_data)
        y = len(mod_data)
        if y>2:
            mod_data.to_csv(f"analysed/{stock}.csv",index = False)
            mod_data = mod_data[(mod_data["Open_pc"]<=3) & (mod_data["Close_pc"]<=3) & (mod_data["High_pc"]<=3) & (mod_data["Low_pc"]<=3)]
            l1 = len(mod_data)
            fhand1.write(f"{stock}, {y}, {l1}\n")
            if y - l1>=200:
                fhand.write(f"{stock} is having {y} - anomoly stock\n")
            mod_data.to_csv(f"modified/{stock}.csv",index = False)
        else:
            fhand1.write(f"{stock}, {y}, No common data ---------\n")
scrip_dict = {'AARTIIND': 7, 'ABB': 13, 'ACC': 22, 'ADANIENT': 25, 'AEGISCHEM': 40, 'HAPPSTMNDS': 48, 
              'ARE&M': 100, 'ROUTE': 128, 'APOLLOHOSP': 157, 'APOLLOTYRE': 163, 'ASHOKLEY': 212, 'IEX': 220, 
              'ASIANPAINT': 236, 'ATUL': 263, 'AUROPHARMA': 275, 'GICRE': 277, 'BAJAJHLDNG': 305, 
              'TIINDIA': 312, 'BAJFINANCE': 317, 'ANGELONE': 324, 'BALKRISIND': 335, 'BALRAMCHIN': 341, 
              'CAMS': 342, 'NAM-INDIA': 357, 'BATAINDIA': 371, 'BBTC': 380, 'BEL': 383, 'BEML': 395, 
              'NIACL': 399, 'BERGEPAINT': 404, 'BHARATFORG': 422, 'BHEL': 438, 'HDFCLIFE': 467, 
              '3MINDIA': 474, 'BIRLACORPN': 480, 'BLUEDART': 495, 'SHRIRAMFIN': 17179, 'MAZDOCK': 509, 
              'BPCL': 526, 'UTIAMC': 527, 'BRITANNIA': 5066, 'CANFINHOME': 583, 'IRFC': 31128, 'GRAPHITE': 592, 
              'CARBORUNIV': 595, 'CENTURYTEX': 625, 'CESC': 628, 'CHAMBLFERT': 637, 'EXIDEIND': 676, 'CHOLAFIN': 21325, 
              'CIPLA': 694, 'COROMANDEL': 739, 'CRISIL': 757, 'CGPOWER': 760, 'DABUR': 772, 'DCMSHRIRAM': 811, 
              'DEEPAKFERT': 827, 'DRREDDY': 881, 'EICHERMOT': 910, 'EQUITASBNK': 913, 'EIDPARRY': 916, 'EIHOTEL': 919, 
              'ELGIEQUIP': 937, 'PGHL': 940, 'ESCORTS': 958, 'EPL': 981, 'FACT': 1008, 'SCHAEFFLER': 1011, 
              'FEDERALBNK': 1023, 'FINCABLES': 1038, 'FINPIPE': 1041, 'ZENSARTECH': 1076, 'GLAXO': 1153, 
              'GNFC': 1174, 'GODFRYPHLP': 1181, 'AMBER': 1185, 'GLAND': 1186, 'KANSAINER': 1196, 'GRASIM': 1232, 
              'GSFC': 1247, 'CASTROLIND': 1250, 'GUJALKALI': 1267, 'AMBUJACEM': 1270, 'GALAXYSURF': 1315, 'HDFCBANK': 20753, 
              'HEG': 1336, 'HEROMOTOCO': 1348, 'HINDALCO': 1363, 'HINDUNILVR': 1394, 'HINDPETRO': 1406, 'HINDZINC': 1424, 
              'SANOFI': 1442, 'IDBI': 1476, 'RBA': 1494, 'ASTERDM': 1508, 'INDHOTEL': 1512, 'INDIACEM': 1515, 
              'GMMPFAUDLR': 1570, 'GILLETTE': 1576, 'INFY': 1594, 'INGERRAND': 1597, 'TATAINVEST': 1621, 
              'IOC': 1624, 'LINDEINDIA': 1627, 'IPCALAB': 1633, 'ITC': 1660, 'ITI': 1675, 'JBCHEPHARM': 1726, 
              'KAJARIACER': 1808, 'KPIL': 1814, 'KARURVYSYA': 1838, 'CUMMINSIND': 1901, 'KOTAKBANK': 1922, 'KSB': 1949, 
              'TRENT': 1964, 'LAXMIMACH': 1979, 'LICHSGFIN': 1997, 'M&M': 2031, 'RAMCOCEM': 2043, 'INDIGOPNTS': 2048, 'HOMEFIRST': 2056, 'VTL': 2073, 'MASTEK': 2124, 'MFSL': 2142, 'BDL': 2144, 'BOSCHLTD': 2181, 'BANDHANBNK': 2263, 'MRF': 2277, 'MRPL': 2283, 'HLEGLAS': 2289, 'HAL': 2303, 'NCC': 2319, 'PEL': 19861, 'ONGC': 2475, 'ISEC': 2489, 'PGHH': 2535, 'LEMONTREE': 2606, 'PFIZER': 2643, 'PCBL': 2649, 'PIDILITIND': 2664, 'POLYPLEX': 2687, 'PRAJIND': 2705, 'MTARTECH': 2709, 'PRSMJOHNSN': 2739, 'JUBLINGREA': 2783, 'EASEMYTRIP': 2792, 'RALLIS': 2816, 'ANURAS': 2829, 'LXCHEM': 2841, 'CRAFTSMAN': 2854, 'RAYMOND': 2859, 'RCF': 2866, 'IIFL': 19069, 'RELIANCE': 2885, 'RECLTD': 30992, 'KALYANKJIL': 2955, 'SAIL': 2963, 'ORIENTELEC': 2972, 'NAZARA': 2987, 'JINDALSAW': 3024, 'SBIN': 3045, 'VEDL': 3063, 'SHREECEM': 3103, 'BORORENEW': 3149, 'SIEMENS': 3150, 'SKFINDIA': 3186, 'LODHA': 3220, 'SRF': 3273, 'SUNDARMFIN': 3339, 'SUNDRMFAST': 3345, 'SUNPHARMA': 3351, 'SUPREMEIND': 3363, 'TATACHEM': 3405, 'TATAELXSI': 3411, 'HONAUT': 3417, 'TATAPOWER': 3426, 'TATACONSUM': 3432, 'TATAMOTORS': 3456, 'THERMAX': 3475, 'TATASTEEL': 3499, 'TITAN': 3506, 'TORNTPHARM': 3518, 'TTKPRESTIG': 3546, 'ADANIGREEN': 3563, 'JUBLPHARMA': 3637, 'VIPIND': 3703, 'VOLTAS': 3718, 'TATACOMM': 3721, 'FINEORG': 3744, 'RITES': 3761, 'WIPRO': 3787, 'ZEEL': 3812, 'VARROC': 3857, 'NATCOPHARM': 3918, 'MARICO': 4067, 'MOTHERSON': 4204, 'HDFCAMC': 4244, 'CREDITACC': 18498, 'AARTIDRUGS': 4481, 'ALKYLAMINE': 4487, 'MPHASIS': 4503, 'BANKBARODA': 4668, 'SONACOMS': 4684, 'SHYAMMETL': 4693, 'GAIL': 4717, 'BANKINDIA': 4745, 'CONCOR': 4749, 'KIMS': 4847, 'SAREGAMA': 4892, 'FDC': 4898, 'ENGINERSIN': 4907, 'ICICIBANK': 4963, 'IRCON': 4986, 'SHARDACROP': 4992, 'CLEAN': 5049, 'GRINFRA': 5054, 'ZOMATO': 5097, 'INDUSINDBK': 5258, 'GLS': 5265, 'DEVYANI': 5373, 'ASAHIINDIA': 5378, 'EPIGRAL': 5382, 'AAVAS': 5385, 'NUVOCO': 5426, 'APTUS': 5435, 'CHEMPLASTS': 5449, 'VIJAYA': 5585, 'CUB': 5701, 'CYIENT': 5748, 'IBULHSGFIN': 30125, 'AXISBANK': 5900, 'INTELLECT': 5926, 'ATGL': 6066, 'NATIONALUM': 6364, 'NYKAA': 6545, 'SONATSOFTW': 6596, 'POLICYBZR': 6656, 'PAYTM': 6705, 'SAPPHIRE': 6718, 'JINDALSTEL': 6733, 'LATENTVIEW': 6818, 'GOCOLORS': 6964, 'BSOFT': 6994, 'STARHEALTH': 7083, 'MAPMYINDIA': 7227, 'HCLTECH': 7229, 'METROBRAND': 7242, 'MEDPLUS': 7254, 'DATAPATTNS': 7358, 'NTPC': 31768, 'RAJESHEXPO': 7401, 'GLENMARK': 7406, 'ZYDUSLIFE': 7929, 'AVANTIFEED': 7936, 'MAHLIFE': 8050, 'DALBHARAT': 8075, 'AWL': 8110, 'AJANTPHARM': 8124, 'MANYAVAR': 8167, 'BLUESTARCO': 8311, 'TVSMOTOR': 8479, 'CHALET': 8546, 'NLCINDIA': 8585, 'MSUMI': 8596, 'GAEL': 8828, 'BCG': 8833, 'USHAMART': 8840, 'TTML': 8954, 'STLTECH': 9309, 'IOB': 9348, 'CAMPUS': 9362, 'PNCINFRA': 9385, 'RAINBOW': 9408, 'LICI': 9480, 'RVNL': 9552, 'METROPOLIS': 9581, 'POLYCAB': 9590, 'DELHIVERY': 9599, 'KPITTECH': 9683, 'TRIDENT': 9685, 'AETHER': 9810, 'HAVELLS': 9819, 'POONAWALLA': 11403, 'GODREJCP': 10099, 'ADANIENSOL': 10217, 'SYNGENE': 10243, 'LUPIN': 10440, 'MCDOWELL-N': 10447, 'KRBL': 10577, 'GUJGASLTD': 10599, 'BHARTIARTL': 10604, 'OLECTRA': 10637, 'PNB': 10666, 'INDIAMART': 10726, 'OFSS': 10738, 'UNIONBANK': 10753, 'SYRMA': 10793, 'CANBK': 10794, 'GODREJIND': 10925, 'DIVISLAB': 10940, 'RADICO': 10990, 'MARUTI': 10999, 'IDFCFIRSTB': 11184, 'INDIGO': 11195, 'UCOBANK': 11223, 'JSL': 11236, 'WELSPUNLIV': 11253, 'IGL': 11262, 'UPL': 11287, 'LUXIND': 11301, 'AFFLE': 11343, 'PETRONET': 11351, 'VAIBHAVGBL': 11364, 'BIOCON': 11373, 'MAHABANK': 11377, 'CCL': 11452, 'LT': 11483, 'APARINDS': 11491, 'ABFRL': 30108, 'ULTRACEMCO': 11532, 'TCS': 11536, 'COFORGE': 11543, 'PPLPHARMA': 11571, 'WESTLIFE': 11580, 'LALPATHLAB': 11654, 'JBMA': 11655, 'SUPRAJIT': 11689, 'ALKEM': 11703, 'JSWSTEEL': 11723, 'SHOPERSTOP': 11813, 'WELCORP': 11821, 'NH': 11840, 'JKPAPER': 11860, 'GRANULES': 11872, 'YESBANK': 11915, 'MEDANTA': 11956, 'IDFC': 11957, 'BIKAJI': 11966, 'SUZLON': 12018, 'ACI': 12024, 'RENUKA': 12026, 'FIVESTAR': 12032, 'KAYNES': 12092, 'SWSOLAR': 12489, 'TEAMLEASE': 12716, 'SAFARI': 13035, '360ONE': 13061, 'TRIVENI': 13081, 'AIAENG': 13086, 'PVRINOX': 13147, 'GSPL': 13197, 'KEC': 13260, 'JKCEMENT': 13270, 'M&MFIN': 20050, 'CENTURYPLY': 13305, 'KEI': 13310, 'SOLARINDS': 13332, 'KFINTECH': 13359, 'SUNTV': 13404, 'GPIL': 13409, 'RATNAMANI': 13451, 'JKLAKSHMI': 13491, 'ALLCARGO': 13501, 'EMAMILTD': 13517, 'GMRINFRA': 13528, 'TECHM': 13538, 'GRINDWELL': 13560, 'IRCTC': 13611, 'JMFINANCIL': 13637, 'FLUOROCHEM': 13750, 'NAUKRI': 13751, 'GESHIP': 13776, 'TORNTPOWER': 21684, 'SOBHA': 13826, 'TANLA': 13976, 'NETWORK18': 14111, 'UNOMINDA': 14154, 'NSLNISP': 14180, 'TIMKEN': 14198, 'TV18BRDCST': 14208, 'REDINGTON': 14255, 'PFC': 20028, 'FSL': 14304, 'INDIANB': 14309, 'IDEA': 14366, 'PAGEIND': 14413, 'ASTRAL': 14418, 'BALAMINES': 14501, 'PHOENIXLTD': 14552, 'FORTIS': 14592, 'NAVINFLUOR': 14672, 'DLF': 14732, 'SPARC': 14788, 'CENTRALBK': 14894, 'KPRMILL': 14912, 'CIEINDIA': 14937, 'MOTILALOFS': 14947, 'CSBBANK': 14966, 'POWERGRID': 21315, 'CERA': 15039, 'DELTACORP': 15044, 'ADANIPORTS': 15083, 'COLPAL': 15141, 'JYOTHYLAB': 15146, 'ECLERX': 15179, 'BRIGADE': 15184, 'UJJIVANSFB': 15228, 'CEATLTD': 15254, 'KNRCON': 15283, 'IRB': 15313, 'NMDC': 15332, 'RAIN': 15337, 'VGUARD': 15362, 'MANKIND': 15380, 'PRINCEPIPE': 16045, 'INFIBEAM': 16249, 'BAJAJ-AUTO': 16669, 'BAJAJFINSV': 16675, 'UBL': 16713, 'ZFCVINDIA': 16915, 'TATAMTRDVR': 16965, 'PATANJALI': 17029, 'CROMPTON': 17094, 'SUMICHEM': 17105, 'BLS': 17279, 'MHRIL': 17333, 'VINATIORGA': 17364, 'ADANIPOWER': 17392, 'NHPC': 31487, 'OIL': 17438, 'MGL': 17534, 'ZYDUSWELL': 17635, 'SUNTECK': 17641, 'ALOKINDS': 17675, 'QUESS': 17704, 'LTIM': 17818, 'JSWENERGY': 17869, 'GODREJPROP': 17875, 'ABBOTINDIA': 17903, 'BAYERCROP': 17927, 'HINDCOPPER': 17939, 'SUVENPHAR': 17945, 'MMTC': 17957, 'NESTLEIND': 17963, 'SBICARD': 17971, 'WHIRLPOOL': 18011, 'CONCORDBIO': 18060, 'JUBLFOOD': 18096, 'PERSISTENT': 18365, 'RBLBANK': 18391, 'POWERINDIA': 18457, 'LTTS': 18564, 'ICICIPRULI': 18652, 'ENDURANCE': 18822, 'SJVN': 18883, 'PNBHOUSING': 20903, 'VBL': 18921, 'MANAPPURAM': 19061, 'SFL': 19184, 'LAURUSLABS': 19234, 'ROSSARI': 19410, 'BSE': 19585, 'GPPL': 19731, 'DMART': 19913, 'HUDCO': 31240, 'DEEPAKNTR': 19943, 'OBEROIRLTY': 20242, 'PRESTIGE': 20302, 'CGCL': 20329, 'COALINDIA': 20374, 'MUTHOOTFIN': 23650, 'JAMNAAUTO': 20778, 'TEJASNET': 21131, 'ERIS': 21154, 'CDSL': 21174, 'AUBANK': 21238, 'COCHINSHIP': 21508, 'ABCAPITAL': 21614, 'DIXON': 21690, 'CHOLAHLDNG': 21740, 'ICICIGI': 21770, 'SBILIFE': 21808, 'HFCL': 21951, 'MAXHEALTH': 22377, 'PIIND': 24184, 'SYMPHONY': 24190, 'RELAXO': 24225, 'L&TFH': 24948, 'APLLTD': 25328, 'TRITURBINE': 25584, 'POLYMED': 25718, 'APLAPOLLO': 25780, 'MINDACORP': 25897, 'SWANENERGY': 27095, 'RTNINDIA': 27297, 'INDUSTOWER': 29135, 
              'VMART': 29284, 'JUSTDIAL': 29962, 'RHIM': 31163, 'MCX': 31181, 'NBCC': 31415}.keys()
i = 1
for stock in scrip_dict:
    print(i,stock)
    callthis(stock)
    i+=1


end_time = time.time()

gmt_time = datetime.utcfromtimestamp(start_time)

# Format the time in HH:MM:SS format
start_time = gmt_time.strftime('%H:%M:%S')


gmt_time = datetime.utcfromtimestamp(end_time)
end_time = gmt_time.strftime('%H:%M:%S')

fhand.write(f"{start_time}, {end_time}\n")