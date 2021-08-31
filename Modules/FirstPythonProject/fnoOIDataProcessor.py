import requests
import os
from datetime import datetime
import zipfile
import csv

def processDeliveryData(es):
    try:
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
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        fnoFileName = 'fo' + dayStr + months[month - 1] + yearStr + 'bhav'
        fnoStocks = []
        recordDate = monthStr + '/' + dayStr + '/' + yearStr + ' 00:00:01.099000'
        fnoUrl = 'https://archives.nseindia.com/content/historical/DERIVATIVES/' + yearStr + '/' + months[month - 1] + '/' + fnoFileName + '.csv.zip'
        print(fnoUrl)
        r1 = requests.get(fnoUrl, allow_redirects=True)
        if (r1.status_code == 200):
            open(fnoFileName + '.csv.zip', 'wb').write(r1.content)
            with zipfile.ZipFile(fnoFileName + '.csv.zip', 'r') as zip_ref:
                zip_ref.extractall('./')
            print('Processed File')
            # initializing the titles and rows list
            fields = []
            rows = []

            # reading csv file
            with open(fnoFileName + '.csv', 'r') as csvfile:
                # creating a csv reader object
                csvreader = csv.reader(csvfile)

                # extracting field names through first row
                fields = next(csvreader)

                # extracting each data row one by one
                for row in csvreader:
                    rows.append(row)

            processed_lines = 0;

            for row in rows:
                # parsing each column of a row
                colCounter = 0
                doc = {}
                columnsValue = []
                open_int_column_index = 0
                chg_in_oi_index = 0
                symbolNameIndex = 0
                strike_pr_index = 0
                for col in row:
                    columnTitle = fields[colCounter]
                    colStripedTitle = columnTitle.strip()
                    if (colStripedTitle == 'OPEN_INT'):
                        open_int_column_index = colCounter
                    if (colStripedTitle == 'CHG_IN_OI'):
                        chg_in_oi_index = colCounter
                    if (colStripedTitle == 'SYMBOL'):
                        symbolNameIndex = colCounter
                    if (colStripedTitle == 'STRIKE_PR'):
                        strike_pr_index = colCounter
                    symbolName = ''
                    if (colCounter == symbolNameIndex):
                        symbolName = col

                    columnsValue.append(col)
                    if (colStripedTitle != ''):
                        if (
                                colCounter == open_int_column_index or colCounter == chg_in_oi_index or colCounter == strike_pr_index):
                            try:
                                doc[colStripedTitle] = int(col)
                            except:
                                doc[colStripedTitle] = 0
                        else:
                            doc[colStripedTitle] = col

                    if symbolName.strip() not in fnoStocks:
                        fnoStocks.append(symbolName.strip())
                    colCounter = colCounter + 1

                if (columnsValue[open_int_column_index] != "0" and columnsValue[chg_in_oi_index] != "0"):
                    doc['executionDate'] = datetime.now()
                    date_time_obj = datetime.strptime(recordDate, '%m/%d/%Y %H:%M:%S.%f')
                    doc['recordDate'] = date_time_obj
                    processed_lines += 1
                    res = es.index(index="eod-fno-oi-data", body=doc)
            print(f"processed_lines {str(processed_lines)} openIntCol {open_int_column_index} chginoiIndex {chg_in_oi_index}")
            return fnoStocks
    finally:
        os.remove(fnoFileName + '.csv')
        os.remove(fnoFileName + '.csv.zip')
        print(f"completed {fnoFileName}")