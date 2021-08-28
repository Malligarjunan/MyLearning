import csv
from datetime import datetime
from elasticsearch import Elasticsearch

stocksFileName = 'PMAnalysis_with_Live_EOD.csv'

try:
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    # initializing the titles and rows list
    fields = []
    rows = []

    # reading csv file
    with open(stocksFileName, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)

        # extracting field names through first row
        fields = next(csvreader)

        # extracting each data row one by one
        for row in csvreader:
            doc = {}
            col_counter = 0;
            for col in row:
                if (fields[col_counter] != ''):
                    doc[fields[col_counter]] = col;
                col_counter += 1
            doc['executionDate'] = datetime.now()
            try:
                res = es.index(index="pm_analysis_data", body=doc)
            except:
                print('Error inserting doc')
                print(doc)
except:
    print('error')
