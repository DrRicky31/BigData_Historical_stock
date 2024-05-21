#!/usr/bin/env python3
#Il MAPPER legge i dati delle azioni, estrae le informazioni necessarie (data, ticker dell'azione, prezzo di chiusura)
#e le organizza per calcolare le variazioni annuali.
import sys
import csv
from collections import defaultdict

def calculate_annual_var(prices): #calcola il ritorno annuale data una lista di prezzi di chiusura, prices[0] all'inizio, prices[-1] alla fine
    return ((prices[-1] - prices[0]) / prices[0]) * 100 #divido per il primo prezzo e moltiplico per 100 per ottenere il ritorno in percentuale

def main():
    reader = csv.reader(sys.stdin)
    next(reader)  # skip the header

    stock_data = defaultdict(lambda: defaultdict(list)) #è un dizionario che mappa ogni azienda con i rispettivi prezzi di chiusura per ogni anno

    for row in reader:
        date = row[0]
        stock = row[7] #estraggo ottavo elenento che è l'azione e il sesto che è il prezzo di chiusura e lo converto in numero
        close_price = float(row[5])

        year = date.split('-')[0]
        if int(year) >= 2000:
            stock_data[stock][year].append(close_price)

    for stock, years in stock_data.items():
        annual_returns = {}
        for year, prices in years.items(): #restituisce tutte le coppie anno-lista di prezzi di chiusura per l'azione
            if len(prices) > 1: #Controlla se la lista di prezzi di chiusura contiene più di un prezzo. 
                                #Questo è importante perché la variazione annuale richiede almeno due prezzi per calcolare la differenza (inizio anno e fine anno).
                annual_returns[year] = calculate_annual_var(prices)

        sorted_years = sorted(annual_returns.keys()) #per ogni azienda, calcola i ritorni annuali e li ordina per anno
        trends = []
        for i in range(len(sorted_years) - 1): #permette di accedere ad ogni anno e al successivo per calcolare il trend tra di essi
            year1 = sorted_years[i] #identificare anno iniziale e finale
            year2 = sorted_years[i + 1]
            if year1 in annual_returns and year2 in annual_returns: #garantire che esistano ritorni annuali calcolati per entrambi gli anni prima di calcolare il trend
                trend = round(annual_returns[year2] - annual_returns[year1], 2)
                trends.append((year1, year2, trend))

        for i in range(len(trends) - 2): #confronta i ritorni annuali di anni consecutivi per ottenere i trend
            trend1 = trends[i]
            trend2 = trends[i + 1]
            trend3 = trends[i + 2]
            if trend1[2] == trend2[2] == trend3[2]:
                print(f"{trend1[2]}\t{stock}\t{trend1[0]}-{trend1[1]},{trend2[0]}-{trend2[1]},{trend3[0]}-{trend3[1]}")

if __name__ == "__main__":
    main()

