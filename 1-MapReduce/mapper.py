#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

# Costruisci il dizionario stocks raccogliendo le informazioni dal file historical_stocks.csv
stocks = {}
with open('../historical_stock_pulito.csv', mode='r') as infile:
    reader = csv.reader(infile)
    next(reader)  # Salta la riga di intestazione
    for row in reader:
        ticker = row[0]
        company_name = row[2]
        stocks[ticker] = company_name

# Leggere il file CSV dallo standard input
reader = csv.reader(sys.stdin)
next(reader)  # Salta la riga di intestazione

for row in reader:
    try:
        ticker = row[0]
        close = float(row[2])
        date_str = row[7]
        low = float(row[4])
        high = float(row[5])
        volume = int(row[6])
        company_name = stocks.get(ticker, "Unknown")
        date = datetime.strptime(date_str, '%Y-%m-%d')
        month= date.month
        year = date.year
        print(f"{ticker}\t{company_name}\t{year}\t{month}\t{close}\t{low}\t{high}\t{volume}")
    except Exception as e:
        # Ignora le righe non valide
        continue
