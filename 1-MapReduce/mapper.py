#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

# Leggere il file CSV dallo standard input
reader = csv.reader(sys.stdin)
next(reader)  # Salta la riga di intestazione
for row in reader:
    try:
        fields = row
        date = datetime.strptime(fields[7], '%Y-%m-%d')
        year = date.year
        ticker = fields[0]
        close = float(fields[2])
        low = float(fields[4])
        high = float(fields[5])
        volume = int(fields[6])
        
        # Emissione dei dati per il reducer
        print(f"{ticker}\t{year}\t{close}\t{low}\t{high}\t{volume}")
    except Exception as e:
        # Ignora le righe non valide
        continue
