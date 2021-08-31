from datetime import datetime
from elasticsearch import Elasticsearch

import deliveryDataProcessor
import fnoOIDataProcessor

daysList = ['31']
dayInt = datetime.now().day
dayStr = str(dayInt)
if dayInt < 10:
    dayStr = '0' + dayStr
month = datetime.now().month
monthStr = str(month)
if month < 10:
    monthStr = '0' + monthStr
year = datetime.now().year
yearStr = str(year)

stocksFileName = dayStr + monthStr + yearStr + '.csv'
print(stocksFileName)

try:
    es = Elasticsearch([{'host': 'localhost', 'port': 9240}])
    fnoStocks = fnoOIDataProcessor.processDeliveryData(es)
    deliveryDataProcessor.processDeliveryData(es, stocksFileName, fnoStocks)

finally:
    print('-------------------Process Completed-----------')
