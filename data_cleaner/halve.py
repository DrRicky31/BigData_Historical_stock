import pandas as pd

def halve_rows(file_path):
    # Legge il file CSV in un DataFrame pandas
    df = pd.read_csv(file_path)
    
    # Calcola l'indice delle righe da mantenere (metà delle righe)
    half_index = len(df) // 2
    df_halved = df.iloc[:half_index]
    
    # Salva il DataFrame modificato in un nuovo file CSV
    output_file_path = file_path.replace("_pulito.csv", "_half.csv")
    df_halved.to_csv(output_file_path, index=False)
    print(f"File salvato come {output_file_path}")

# Esempio di utilizzo
halve_rows("historical_stock_prices_pulito.csv")

