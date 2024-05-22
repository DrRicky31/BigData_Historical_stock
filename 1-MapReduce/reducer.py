#!/usr/bin/env python3
import sys
import csv
import statistics
from collections import defaultdict
from datetime import datetime

# Funzione per calcolare e stampare le statistiche
def calculate_and_output_stats(ticker, year, data):
    data.sort(key=lambda x: x[0])  # Ordina per data
    first_close = data[0][1]
    last_close = data[-1][1]
    pct_change = round((last_close - first_close) / first_close * 100, 2)
    closes = [x[1] for x in data]
    lows = [x[2] for x in data]
    highs = [x[3] for x in data]
    volumes = [x[4] for x in data]
    min_price = min(lows)
    max_price = max(highs)
    avg_volume = round(statistics.mean(volumes), 2)
    print(f"{ticker}\t{company_name}\t{year}\t{pct_change}\t{min_price}\t{max_price}\t{avg_volume}")

# Variabili per tracciare l'attuale ticker e anno
current_ticker = None
current_year = None
year_data = defaultdict(list)

# Processa l'input
for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    company_name=fields[1]
    year = int(fields[2])
    date_str = fields[3]
    close = float(fields[4])
    low = float(fields[5])
    high = float(fields[6])
    volume = int(fields[7])

    if current_ticker != ticker or current_year != year:
        if current_ticker and current_year:
            calculate_and_output_stats(current_ticker, current_year, year_data[(current_ticker, current_year)])
        current_ticker = ticker
        current_year = year

    year_data[(ticker, year)].append((datetime.strptime(date_str, '%Y-%m-%d'), close, low, high, volume))

# Elabora l'ultimo ticker/anno
if current_ticker and current_year:
    calculate_and_output_stats(current_ticker, current_year, year_data[(current_ticker, current_year)])
