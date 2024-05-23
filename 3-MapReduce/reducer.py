#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dizionario per raccogliere i dati per ciascuna azione e anno
action_data = defaultdict(lambda: {'company_name': None, 'prices': [], 'first_close': None, 'last_close': None})
pct_changes = defaultdict(lambda: defaultdict(lambda: None))  # pct_changes[ticker][year] = pct_change

# Processa l'input dallo standard input
for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    company_name = fields[1]
    year = int(fields[2])
    month = int(fields[3])
    close = float(fields[4])

    if action_data[(ticker, year)]['company_name'] is None:
        action_data[(ticker, year)]['company_name'] = company_name
    
    
    if month <= 6:
        if action_data[(ticker, year)]['first_close'] is None:
            action_data[(ticker, year)]['first_close'] = close
    else:
        action_data[(ticker, year)]['last_close'] = close

# Calcola le variazioni percentuali per ciascuna azione e anno
for (ticker, year), data in action_data.items():
    first_close = data['first_close']
    last_close = data['last_close']
    
    if first_close is not None and last_close is not None:
        pct_change = round((last_close - first_close) / first_close * 100)
        pct_changes[ticker][year] = pct_change

# Trova gruppi di aziende con lo stesso trend per almeno tre anni consecutivi
trend_groups = defaultdict(list)  # trend_groups[(year1, year2, year3, ...)]['trend'] = [company1, company2, ...]

for ticker, yearly_changes in pct_changes.items():
    sorted_years = sorted(yearly_changes.keys())
    trend_sequence = []
    
    for i in range(len(sorted_years) - 2):  # Guardiamo almeno tre anni consecutivi
        y1, y2, y3 = sorted_years[i], sorted_years[i+1], sorted_years[i+2]
        trend1, trend2, trend3 = yearly_changes[y1], yearly_changes[y2], yearly_changes[y3]
        
        if trend1 == trend2 == trend3:  # Se il trend è lo stesso per tre anni consecutivi
            trend_sequence.append((y1, y2, y3, trend1))
    
    # Aggiungi al gruppo se abbiamo una sequenza valida
    for sequence in trend_sequence:
        trend_groups[sequence].append(action_data[(ticker, sequence[0])]['company_name'])

# Stampa i gruppi di aziende con lo stesso trend per almeno tre anni consecutivi
for sequence, companies in trend_groups.items():
    if len(companies) > 1:  # Assicurati che ci siano almeno due aziende nel gruppo
        years, trend = sequence[:-1], sequence[-1]
        years_str = ",".join(map(str, years))
        companies_str = ", ".join(companies)
        print(f"{years_str}: {trend}%: {companies_str}")
