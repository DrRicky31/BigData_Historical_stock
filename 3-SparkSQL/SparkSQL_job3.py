from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, round as spark_round, lag, lead, concat, lit, collect_list, array_join
from pyspark.sql.window import Window

# Inizializza una sessione Spark
spark = SparkSession.builder.appName("Stock Analysis").getOrCreate()

# Carica i dati
historical_stocks_df = spark.read.csv('/user/hadoop/historical_stock_pulito.csv', header=True, inferSchema=True)
historical_prices_df = spark.read.csv('/user/hadoop/historical_stock_prices_pulito.csv', header=True, inferSchema=True)

# Unisci i dati dei prezzi con i nomi delle aziende
joined_df = historical_prices_df.join(historical_stocks_df, "ticker") \
    .withColumn("year", col("date").substr(1, 4).cast("int")) \
    .filter(col("year") > 2000)

# Calcola le aggregazioni richieste per ciascun ticker e anno
aggregated_df = joined_df.groupBy("ticker", "name", "year") \
    .agg(
        first("close").alias("first_close"),
        last("close").alias("last_close")
    ) \
    .withColumn("pct_change", spark_round((col("last_close") - col("first_close")) / col("first_close") * 100, 0)) \
    .withColumn("pct_change", concat(col("pct_change").cast("int"), lit("%"))) \
    .select("ticker", "name", "year", "pct_change")

# Definisci la finestra di partizione per ticker e ordina per anno
window_spec = Window.partitionBy("ticker").orderBy("year")

# Calcola la variazione percentuale dei tre anni consecutivi
consecutive_df = aggregated_df \
    .withColumn("pct_change_lag1", lag("pct_change", 1).over(window_spec)) \
    .withColumn("pct_change_lag2", lag("pct_change", 2).over(window_spec)) \
    .filter((col("pct_change") == col("pct_change_lag1")) & (col("pct_change") == col("pct_change_lag2"))) \
    .select("year", lead("year", 1).over(window_spec).alias("year2"), lead("year", 2).over(window_spec).alias("year3"), "pct_change", "name")

# Filtra per avere solo anni validi
filtered_df = consecutive_df.filter(col("year3").isNotNull())

# Raggruppa le aziende che hanno la stessa percentuale per tre anni consecutivi
grouped_df = filtered_df.groupBy("year", "year2", "year3", "pct_change") \
    .agg(array_join(collect_list("name"), ", ").alias("company_names"))

# Ordina per anno e percentuale di cambiamento
final_df = grouped_df.orderBy("year", "pct_change")

# Mostra il risultato
final_df.show(truncate=False)

# Salva il risultato su un file CSV in una singola partizione
final_df.coalesce(1).write.csv('/user/hadoop3/SQL3_result', header=True, mode='overwrite')
