import requests
import csv
import os
from datetime import datetime
from elasticsearch import Elasticsearch
import zipfile

months = ['JAN','FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
fnoStocks = []
dayInt = datetime.now().day
dayStr = str(dayInt)
if dayInt < 10:
    dayStr = '0' + dayStr
dayStr = '27'
month = datetime.now().month
monthStr = str(month)
if month < 10:
    monthStr = '0' + monthStr
year = datetime.now().year
yearStr = str(year)
print(months[month - 1])
stocksFileName = dayStr + monthStr + yearStr + '.csv'
print(stocksFileName)
fnoFileName = 'fo' + dayStr + months[month - 1] + yearStr + 'bhav'

try:
    es = Elasticsearch([{'host': 'localhost', 'port': 9240}])
    fnoUrl = 'https://archives.nseindia.com/content/historical/DERIVATIVES/'+yearStr+'/'+months[month-1]+'/'+fnoFileName+'.csv.zip'
    url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_'+stocksFileName
    print(url)
    print(fnoUrl)
    r1 = requests.get(fnoUrl, allow_redirects=True)
    open(fnoFileName+'.csv.zip', 'wb').write(r1.content)
    with zipfile.ZipFile(fnoFileName+'.csv.zip', 'r') as zip_ref:
        zip_ref.extractall('./')
    r = requests.get(url, allow_redirects=True)
    open(stocksFileName, 'wb').write(r.content)

    # initializing the titles and rows list
    fields = []
    rows = []

    # reading csv file
    with open(fnoFileName+'.csv', 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)

        # extracting field names through first row
        fields = next(csvreader)

        # extracting each data row one by one
        for row in csvreader:
            rows.append(row)

    processed_lines = 0;
    line_count = 0;
    for row in rows:
        # parsing each column of a row
        colCounter = 0
        doc = {}
        columnsValue = []
        open_int_column_index = 0
        chg_in_oi_index = 0
        symbolNameIndex = 0
        for col in row:
            columnTitle = fields[colCounter]
            columnsValue.append(col)
            if (columnTitle.strip() == 'OPEN_INT'):
                open_int_column_index = colCounter
            if (columnTitle.strip() == 'CHG_IN_OI'):
                chg_in_oi_index = colCounter
            if (columnTitle.strip() == 'SYMBOL'):
                symbolNameIndex = colCounter
            symbolName = ''
            if (colCounter == symbolNameIndex):
                symbolName = col
            colCounter = colCounter + 1
            if (columnTitle.strip() != ''):
                doc[columnTitle.strip()] = col

            if symbolName.strip() not in fnoStocks:
                fnoStocks.append(symbolName.strip())
        if (columnsValue[open_int_column_index] != 0 and columnsValue[chg_in_oi_index] != 0):
            doc['executionDate'] = datetime.now()
            processed_lines += 1
            res = es.index(index="fno-stocks-data", body=doc)

    with open(stocksFileName) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        fields = []
        rows = []
        fields = next(csv_reader)

        for row in csv_reader:
            if (row[14].strip() != '-' and row[0] in fnoStocks):
                doc = {}
                doc[fields[0]] = row[0]
                doc[fields[13]] = row[13]
                doc[fields[14]] = row[14]
                doc['executionDate'] = datetime.now()
                es.index(index="stocks-delivery-data", body=doc)
                line_count += 1



finally:
    os.remove(stocksFileName)
    os.remove(fnoFileName+'.csv')
    os.remove(fnoFileName+'.csv.zip')
    print("complete")
