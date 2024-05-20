#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

stocks = {}

# Caricare il file historical_stocks.csv
with open('historical_stocks_pulito.csv', mode='r') as infile:
    reader = csv.reader(infile)
    next(reader)  # Salta la riga di intestazione
    for rows in reader:
        stocks[rows[0]] = rows[1]

for line in sys.stdin:
    try:
        fields = line.strip().split(',')
        date = datetime.strptime(fields[0], '%Y-%m-%d')
        year = date.year
        ticker = fields[1]
        close = float(fields[5])
        low = float(fields[4])
        high = float(fields[3])
        volume = int(fields[6])
        
        if ticker in stocks:
            print(f"{ticker}\t{year}\t{close}\t{low}\t{high}\t{volume}")
    except:
        continue
