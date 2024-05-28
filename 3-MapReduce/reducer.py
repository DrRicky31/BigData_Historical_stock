#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dizionario per raccogliere i dati per ciascuna azione e anno
action_data = defaultdict(lambda: {'company_name': None, 'prices': [], 'closes' : {}, 'first_close': None, 'last_close': None})
date_data = defaultdict(list)  # Dizionario per raccogliere le date per ciascun ticker e anno
pct_changes = defaultdict(lambda: defaultdict(lambda: None))  # pct_changes[ticker][year] = pct_change

# Processa l'input dallo standard input
for line in sys.stdin:
    fields = line.strip().split('\t')
    ticker = fields[0]
    company_name = fields[1]
    year = int(fields[2])
    date = fields[3]
    close = float(fields[4])

    if action_data[(ticker, year)]['company_name'] is None:
        action_data[(ticker, year)]['company_name'] = company_name
    
     # Aggiungi la data alla lista delle date per quell'anno e ticker
    date_data[(ticker, year)].append(date)
    
    action_data[(ticker, year)]['closes'][date] = close  # Memorizza il valore di chiusura per la data

# Calcola le date minime e massime per ogni ticker e anno
first_dates = {key: min(dates) for key, dates in date_data.items()}
last_dates = {key: max(dates) for key, dates in date_data.items()}

# Calcola le statistiche per ciascuna azione e anno
for (ticker, year), data in action_data.items():
    # Trova le date di inizio e fine per l'anno corrente e il ticker
    first_date = first_dates[(ticker, year)]
    last_date = last_dates[(ticker, year)]
    
    # Recupera i valori di chiusura per le date di inizio e fine
    first_close = data['closes'].get(first_date)
    last_close = data['closes'].get(last_date)
    
    if first_close is not None and last_close is not None:
        pct_change = round((last_close - first_close) / first_close * 100)
    else:
        pct_change = None
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
    
    """
    # Versione con solo due anni consecutivi
    for i in range(len(sorted_years) - 2):  # Guardiamo almeno due anni consecutivi
        y1, y2 = sorted_years[i], sorted_years[i+1]
        trend1, trend2 = yearly_changes[y1], yearly_changes[y2]
        
        if trend1 == trend2:  # Se il trend è lo stesso per due anni consecutivi
            trend_sequence.append((y1, y2, trend1))  
    """

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