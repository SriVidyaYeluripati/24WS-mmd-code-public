from pyspark.sql import SparkSession
from pyspark.sql.functions import length, avg

# Create a Spark session
spark = SparkSession.builder.appName("StructuredNetworkWordLength").getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words
words = lines.selectExpr("explode(split(value, ' ')) as word")

# Calculate word lengths
word_lengths = words.select(length("word").alias("word_length"))

# Calculate count of word lengths and average word length
length_counts = word_lengths.groupBy("word_length").count()
average_length = word_lengths.agg(avg("word_length").alias("avg_length"))

# Start running the queries that print the running counts and average length to the console
query1 = length_counts.writeStream.outputMode("complete").format("console").start()
query2 = average_length.writeStream.outputMode("complete").format("console").start()

query1.awaitTermination()
query2.awaitTermination()
