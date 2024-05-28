from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, first, last, round as spark_round
from pyspark.sql.window import Window

# Inizializza una sessione Spark
spark = SparkSession.builder.appName("Stock Analysis").getOrCreate()

# Carica i dati
historical_stocks_df = spark.read.csv('/user/hadoop/historical_stock_pulito.csv', header=True, inferSchema=True)
historical_prices_df = spark.read.csv('/user/hadoop/historical_stock_prices_pulito.csv', header=True, inferSchema=True)

# Crea una vista temporanea per eseguire query SQL
historical_stocks_df.createOrReplaceTempView("historical_stocks")
historical_prices_df.createOrReplaceTempView("historical_prices")

# Unisci i dati dei prezzi con i nomi delle aziende
joined_df = spark.sql("""
    SELECT
        p.ticker,
        s.name as company_name,
        YEAR(p.date) as year,
        p.date,
        p.close,
        p.low,
        p.high,
        p.volume
    FROM historical_prices p
    LEFT JOIN historical_stocks s ON p.ticker = s.ticker
""")

# Crea una vista temporanea per i dati uniti
joined_df.createOrReplaceTempView("joined_data")

# Calcola le aggregazioni richieste per ciascun ticker e anno
aggregated_df = spark.sql("""
    SELECT
        ticker,
        company_name,
        year,
        MIN(low) as min_price,
        MAX(high) as max_price,
        ROUND(AVG(volume), 2) as avg_volume,
        FIRST(close) as first_close,
        LAST(close) as last_close
    FROM joined_data
    GROUP BY ticker, company_name, year
""")

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
single_partition_df.write.csv('/user/hadoop/result1', header=True, mode='overwrite')
