import random
import pandas as pd

def modify_value(value):
    if isinstance(value, (int, float)):
        return value * random.random()
    else:
        return value

def process_csv(file_path):
    # Legge il file CSV in un DataFrame pandas
    df = pd.read_csv(file_path)
    
    # Raddoppia il DataFrame concatenando il DataFrame con una sua copia
    df_doubled = pd.concat([df, df], ignore_index=True)
    
    # Modifica i valori nel DataFrame raddoppiato
    df_modified = df_doubled.map(modify_value)
    
    # Salva il DataFrame modificato in un nuovo file CSV
    output_file_path = file_path.replace("_pulito.csv", "_double.csv")
    df_modified.to_csv(output_file_path, index=False)
    print(f"File salvato come {output_file_path}")

# Esempio di utilizzo
process_csv("historical_stock_prices_pulito.csv")
process_csv("historical_stock_pulito.csv")
