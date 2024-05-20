#!/usr/bin/env python3
import sys
import statistics

current_ticker = None
current_year = None
closes = []
lows = []
highs = []
volumes = []

stocks = {"AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corporation"}  # Definisci il dizionario "stocks"

def calculate_and_output_stats(ticker, year, closes, lows, highs, volumes):
    first_close = closes[0]
    last_close = closes[-1]
    pct_change = round((last_close - first_close) / first_close * 100, 2)
    min_price = min(lows)
    max_price = max(highs)
    avg_volume = round(statistics.mean(volumes), 2)
    company_name = stocks.get(ticker, "Unknown")
    print(f"{ticker}\t{company_name}\t{year}\t{pct_change}\t{min_price}\t{max_price}\t{avg_volume}")

for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    year = int(fields[1])
    close = float(fields[2])
    low = float(fields[3])
    high = float(fields[4])
    volume = int(fields[5])

    if current_ticker == ticker and current_year == year:
        closes.append(close)
        lows.append(low)
        highs.append(high)
        volumes.append(volume)
    else:
        if current_ticker and current_year:
            calculate_and_output_stats(current_ticker, current_year, closes, lows, highs, volumes)
        
        current_ticker = ticker
        current_year = year
        closes = [close]
        lows = [low]
        highs = [high]
        volumes = [volume]

if current_ticker and current_year:
    calculate_and_output_stats(current_ticker, current_year, closes, lows, highs, volumes)
