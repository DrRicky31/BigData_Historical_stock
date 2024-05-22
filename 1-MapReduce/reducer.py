#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dizionario per raccogliere i dati per ciascuna azione e anno
action_data = defaultdict(lambda: {'prices': [], 'first_close': None, 'last_close': None})

# Processa l'input dallo standard input
for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    company_name = fields[1]
    year = int(fields[2])
    month = int(fields[3])
    close = float(fields[4])
    low = float(fields[5])
    high = float(fields[6])
    volume = int(fields[7])

    action_data[(ticker, year)]['prices'].append((low, high, volume))
    
    if month <= 6:
        if action_data[(ticker, year)]['first_close'] is None:
            action_data[(ticker, year)]['first_close'] = close
    else:
        action_data[(ticker, year)]['last_close'] = close

# Calcola le statistiche per ciascuna azione e anno
for (ticker, year), data in action_data.items():
    first_close = data['first_close']
    last_close = data['last_close']
    
    if first_close is not None and last_close is not None:
        pct_change = round((last_close - first_close) / first_close * 100, 2)
    else:
        pct_change = None
    
    min_price = min(x[0] for x in data['prices'])
    max_price = max(x[1] for x in data['prices'])
    avg_volume = round(sum(x[2] for x in data['prices']) / len(data['prices']), 2)

    print(f"{ticker}\t{company_name}\t{year}\t{pct_change}\t{min_price}\t{max_price}\t{avg_volume}")
