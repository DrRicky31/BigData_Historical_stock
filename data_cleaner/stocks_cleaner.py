import pandas as pd
df = pd.read_csv('historical_stocks.csv')

# Rimuove i valori nulli sulle colonne di interesse
df = df.dropna(subset=['ticker', 'name'])

# Rimuove i duplicati
df = df.drop_duplicates()

# verifica che i simboli e i nomi delle aziende siano validi
df = df[df['ticker'].str.match(r'^[A-Z]+$')]
df = df[df['name'].str.len() > 0]

# Verifica che le stringhe siano in maiuscolo
df['ticker'] = df['ticker'].str.upper()
df['name'] = df['name'].str.upper()

df.to_csv('historical_stock_pulito.csv', index=False)