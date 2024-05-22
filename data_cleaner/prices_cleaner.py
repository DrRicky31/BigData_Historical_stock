import pandas as pd
df = pd.read_csv('historical_stock_prices.csv')

# Rimuovere valori nulli o mancanti
df = df.dropna()

# Filtra i valori con prezzi negativi e con volumi di scambio negativi
df = df[df['close'] > 0]
df = df[df['volume'] > 0]

# gestione outliers
from scipy import stats
import numpy as np
df = df[(np.abs(stats.zscore(df['close'])) < 3)]

# Rimuove duplicati
df = df.drop_duplicates()

# Rimuove righe con date fuori intervallo
df['date'] = pd.to_datetime(df['date'])
df = df[(df['date'] >= '1970-01-01') & (df['date'] <= '2018-12-31')]

# Calcola le date minime e massime per ogni anno
first_dates = df.groupby(df['date'].dt.year)['date'].min()
last_dates = df.groupby(df['date'].dt.year)['date'].max()

# Filtra il DataFrame originale
df = df[df['date'].isin(first_dates) | df['date'].isin(last_dates)]

# Salva il DataFrame pulito in un nuovo file CSV
df.to_csv('historical_stock_prices_pulito.csv', index=False)
