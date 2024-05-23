#!/usr/bin/env python3
import sys
from collections import defaultdict
from datetime import datetime

# Dizionario per raccogliere i dati per ciascuna azione e anno
action_data = defaultdict(lambda: {'company_name': None, 'prices': [], 'closes': {}, 'first_close': None, 'last_close': None})
date_data = defaultdict(list)  # Dizionario per raccogliere le date per ciascun anno

# Processa l'input dallo standard input
for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    company_name = fields[1]
    year = int(fields[2])
    date = fields[3]  # Assumendo che la data sia il quarto campo (indice 3)
    close = float(fields[4])
    low = float(fields[5])
    high = float(fields[6])
    volume = int(fields[7])

    # Aggiungi la data alla lista delle date per quell'anno
    date_data[year].append(date)

    if action_data[(ticker, year)]['company_name'] is None:
        action_data[(ticker, year)]['company_name'] = company_name
    
    action_data[(ticker, year)]['prices'].append((low, high, volume))
    action_data[(ticker, year)]['closes'][date] = close  # Memorizza il valore di chiusura per la data

# Calcola le date minime e massime per ogni anno
first_dates = {year: min(dates) for year, dates in date_data.items()}
last_dates = {year: max(dates) for year, dates in date_data.items()}

# Calcola le statistiche per ciascuna azione e anno
for (ticker, year), data in action_data.items():
    # Trova le date di inizio e fine per l'anno corrente
    first_date = first_dates[year]
    last_date = last_dates[year]
    
    # Recupera i valori di chiusura per le date di inizio e fine
    first_close = data['closes'].get(first_date)
    last_close = data['closes'].get(last_date)
    
    if first_close is not None and last_close is not None:
        pct_change = round((last_close - first_close) / first_close * 100, 2)
    else:
        pct_change = None
    
    min_price = min(x[0] for x in data['prices'])
    max_price = max(x[1] for x in data['prices'])
    avg_volume = round(sum(x[2] for x in data['prices']) / len(data['prices']), 2)
    
    company_name = data['company_name']
    print(f"{ticker}\t{company_name}\t{year}\t{pct_change}\t{min_price}\t{max_price}\t{avg_volume}")
