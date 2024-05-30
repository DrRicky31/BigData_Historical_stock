from pyspark.sql import SparkSession
from pyspark.sql.functions import year, concat_ws, col

# Initialize a Spark session
spark = SparkSession.builder.appName("Stock Analysis").getOrCreate()

<<<<<<< HEAD
# Load the data
=======
# Carica i dati
>>>>>>> refs/remotes/origin/main
historical_stocks_df = spark.read.csv('/user/hadoop/historical_stock_pulito.csv', header=True, inferSchema=True)
historical_prices_df = spark.read.csv('/user/hadoop/historical_stock_prices_pulito.csv', header=True, inferSchema=True)

# Join the price data with the company names
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

# Filter the data from 2000 onwards
filtered_df = joined_df.filter(joined_df.year >= 2000)

# Convert the DataFrame to RDD for aggregation
joined_rdd = filtered_df.rdd.map(lambda row: (
    (row['ticker'], row['company_name'], row['year']),
    (row['date'], row['close'], row['low'], row['high'], row['volume'])
))

print("joined_rdd count: ", joined_rdd.count())

# Function to aggregate the data
def aggregate_data(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    min_price = min(v[2] for v in values)
    max_price = max(v[3] for v in values)
    avg_volume = round(sum(v[4] for v in values) / len(values), 2)
    first_close = sorted_values[0][1]
    last_close = sorted_values[-1][1]
    pct_change = round((last_close - first_close) / first_close * 100)
    return (min_price, max_price, avg_volume, first_close, last_close, pct_change)

# Group by ticker, company_name and year and apply the aggregation
aggregated_rdd = joined_rdd.groupByKey().mapValues(aggregate_data)

print("aggregated_rdd count: ", aggregated_rdd.count())

# Convert the aggregated RDD back into DataFrame
aggregated_df = aggregated_rdd.map(lambda x: (
    x[0][0], x[0][1], x[0][2], x[1][5], x[1][0], x[1][1], x[1][2]
)).toDF(["ticker", "company_name", "year", "pct_change", "min_price", "max_price", "avg_volume"])

# Function to find sequences of similar percentage changes for at least three consecutive years
def find_sequences(data):
    result = []
    sorted_data = sorted(data, key=lambda x: x[2])  # Sort by year
    n = len(sorted_data)
    for i in range(n - 2):
        if (sorted_data[i+1][2] == sorted_data[i][2] + 1 and
            sorted_data[i+2][2] == sorted_data[i][2] + 2 and
            sorted_data[i][3] == sorted_data[i+1][3] == sorted_data[i+2][3]):  # Check that the percentage values are the same
            trend = (sorted_data[i][2], sorted_data[i+1][2], sorted_data[i+2][2],
                     sorted_data[i][3], sorted_data[i+1][3], sorted_data[i+2][3])
            result.append((sorted_data[i][0], sorted_data[i][1], trend))
    return result

# Group by ticker and apply the find_sequences function
ticker_grouped_rdd = aggregated_df.rdd.map(lambda row: (row['ticker'], (row['ticker'], row['company_name'], row['year'], row['pct_change'])))

print("ticker_grouped_rdd count: ", ticker_grouped_rdd.count())

trend_sequences_rdd = ticker_grouped_rdd.groupByKey().flatMap(lambda x: find_sequences(list(x[1])))

print("trend_sequences_rdd count: ", trend_sequences_rdd.count())

# Group by common trend and collect the companies
grouped_trends_rdd = trend_sequences_rdd.map(lambda x: (x[2], x[1])) \
    .groupByKey() \
    .mapValues(list) \
    .filter(lambda x: len(x[1]) > 1)

print("grouped_trends_rdd count: ", grouped_trends_rdd.count())

# Convert to DataFrame
trend_df = grouped_trends_rdd.map(lambda x: (x[0], ', '.join(x[1]))).toDF(["trend", "companies"])

# Convert the 'trend' field from struct to string, including the years
trend_df = trend_df.withColumn("trend", concat_ws(", ", 
                                                  concat_ws(":", col("trend._1"), col("trend._4")), 
                                                  concat_ws(":", col("trend._2"), col("trend._5")), 
                                                  concat_ws(":", col("trend._3"), col("trend._6"))))

# Show the result
trend_df.show(truncate=False)

# Save the result to a CSV file in a single partition
trend_df.coalesce(1).write.csv('/user/hadoop3/SparkCore3_result', header=True, mode='overwrite')