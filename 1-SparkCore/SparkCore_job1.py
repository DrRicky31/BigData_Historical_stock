from pyspark.sql import SparkSession
from pyspark.sql.functions import year

# Inizializza una sessione Spark
spark = SparkSession.builder.appName("Stock Analysis").getOrCreate()

# Carica i dati
historical_stocks_df = spark.read.csv('/user/hadoop/historical_stock_pulito.csv', header=True, inferSchema=True)
historical_prices_df = spark.read.csv('/user/hadoop/historical_stock_prices_pulito.csv', header=True, inferSchema=True)

# Unisci i dati dei prezzi con i nomi delle aziende
joined_df = historical_prices_df.join(historical_stocks_df, historical_prices_df.ticker == historical_stocks_df.ticker, "left") \
    .select(
        historical_prices_df.ticker,
        historical_stocks_df.name.alias("company_name"),
        year(historical_prices_df.date).alias("year"),
        historical_prices_df.date,
        historical_prices_df.close,
        historical_prices_df.low,
        historical_prices_df.high,
        historical_prices_df.volume
    )

# Converti il DataFrame in RDD per l'aggregazione
joined_rdd = joined_df.rdd.map(lambda row: (
    (row['ticker'], row['company_name'], row['year']),
    (row['date'], row['close'], row['low'], row['high'], row['volume'])
))

# Funzione per aggregare i dati
def aggregate_data(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    min_price = min(v[2] for v in values)
    max_price = max(v[3] for v in values)
    avg_volume = round(sum(v[4] for v in values) / len(values), 2)
    first_close = sorted_values[0][1]
    last_close = sorted_values[-1][1]
    pct_change = round((last_close - first_close) / first_close * 100, 2)
    return (min_price, max_price, avg_volume, first_close, last_close, pct_change)

# Raggruppa per ticker, company_name e year e applica l'aggregazione
aggregated_rdd = joined_rdd.groupByKey().mapValues(aggregate_data)

# Converti l'RDD aggregato di nuovo in DataFrame
aggregated_df = aggregated_rdd.map(lambda x: (
    x[0][0], x[0][1], x[0][2], x[1][5], x[1][0], x[1][1], x[1][2]
)).toDF(["ticker", "company_name", "year", "pct_change", "min_price", "max_price", "avg_volume"])

# Riduci il numero di partizioni a una
single_partition_df = aggregated_df.coalesce(1)

# Mostra il risultato
single_partition_df.show()

# Salva il risultato su un file CSV in una singola partizione
single_partition_df.write.csv('/user/hadoop/SparkCore_result', header=True, mode='overwrite')
