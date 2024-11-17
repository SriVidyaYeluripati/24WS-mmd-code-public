from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words
words = lines.selectExpr("explode(split(value, ' ')) as word")

# Generate running word count
word_counts = words.groupBy("word").count()

# Define the output sink
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
    
query.awaitTermination()

