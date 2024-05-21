#!/usr/bin/env python3
#il REDUCER riceve in input i dati emessi dal mapper, li aggrega e li organizza per trovare gruppi di aziende 
#che condividono lo stesso trend di variazione annuale per almeno tre anni consecutivi
import sys
from collections import defaultdict

def main():
    current_trend = None
    companies = defaultdict(list) #mappa ogni trend alle aziende corrispondenti

    for line in sys.stdin: #legge l'input che è l'output del mapper
        trend, company, years = line.strip().split("\t") #separa trend,azienda e anno
        if current_trend is None: #se il trend cambia, verifica se ci sono più di una azienda nel gruppo corrente e stampa il gruppo. 
                                    # Poi resetta companies per il nuovo trend 
            current_trend = trend
        
        if current_trend == trend:  #se il trend è lo stesso, aggiungi l'azienda alla lista
            companies[trend].append((company, years))
        else:
            if len(companies[current_trend]) > 1: #assicurarsi che ci siano abbastanza aziende per formare un gruppo significativo
                print(f"{current_trend}\t{companies[current_trend]}") #stampa il trend corrente e il gruppo di aziende associate
            current_trend = trend
            companies = defaultdict(list) #ripristinare companies per aggregare le aziende del nuovo trend
            companies[trend].append((company, years))
    
    if len(companies[current_trend]) > 1:
        print(f"{current_trend}\t{companies[current_trend]}")

if __name__ == "__main__":
    main()
