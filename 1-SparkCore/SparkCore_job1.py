from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, min as spark_min, max as spark_max, avg, first, last, round as spark_round
from pyspark.sql.window import Window

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

# Calcola le aggregazioni richieste per ciascun ticker e anno
aggregated_df = joined_df \
    .groupBy("ticker", "company_name", "year") \
    .agg(
        spark_min("low").alias("min_price"),
        spark_max("high").alias("max_price"),
        spark_round(avg("volume"), 2).alias("avg_volume"),
        first("close").alias("first_close"),
        last("close").alias("last_close")
    )

# Calcola la variazione percentuale utilizzando le funzioni di finestra
window_spec = Window.partitionBy("ticker", "year").orderBy("date")

result_df = joined_df \
    .withColumn("first_close", first("close").over(window_spec)) \
    .withColumn("last_close", last("close").over(window_spec)) \
    .groupBy("ticker", "company_name", "year") \
    .agg(
        spark_min("low").alias("min_price"),
        spark_max("high").alias("max_price"),
        spark_round(avg("volume"), 2).alias("avg_volume"),
        first("first_close").alias("first_close"),
        last("last_close").alias("last_close")
    ) \
    .withColumn("pct_change", spark_round((col("last_close") - col("first_close")) / col("first_close") * 100, 2)) \
    .select("ticker", "company_name", "year", "pct_change", "min_price", "max_price", "avg_volume")

# Riduci il numero di partizioni a una
single_partition_df = result_df.coalesce(1)

# Mostra il risultato
single_partition_df.show()

# Salva il risultato su un file CSV in una singola partizione
single_partition_df.write.csv('/user/hadoop/SparkCore_result', header=True, mode='overwrite')
