import requests
import os
from datetime import datetime
import csv
def processDeliveryData(es, stocksFileName, fnoStocks):
    try:
        url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_' + stocksFileName
        r = requests.get(url, allow_redirects=True)
        print(r.status_code)
        if (r.status_code != 200):
            open(stocksFileName, 'wb').write(r.content)
            print(f"Processed delivery data file {stocksFileName} ")
            with open(stocksFileName) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                fields = next(csv_reader)
                for row in csv_reader:
                    if (row[14].strip() != '-' and row[0] in fnoStocks):
                        doc = {}
                        doc[fields[0]] = row[0]
                        doc[fields[13]] = int(row[13])
                        doc[fields[14]] = float(row[14])
                        doc['executionDate'] = datetime.now()
                        es.index(index="fno-stocks-delivery-data", body=doc)
                        line_count += 1
    finally:

        os.remove(stocksFileName)
        print(f"completed {stocksFileName}")
