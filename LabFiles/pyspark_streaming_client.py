
# --driver-memory should be at a minimum of 512M, or will choke with an error
# spark-submit --driver-memory 512M pyspark_streaming_client.py

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


 
# Start polling for data on 2 threads every interval_sec seconds
threads = 2
interval_sec = 10

sc = SparkContext("local[" + str(threads) + "]", "PySpark_Streaming")
ssc = StreamingContext(sc, interval_sec)

# Avoid messages below ERROR
sc.setLogLevel("ERROR")

hostname = 'localhost'
port = 9999
# Connect to hostname:port
# Create a DStream (a stream of RDDs)
lines = ssc.socketTextStream(hostname, port)

# Count the words
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
word_count = pairs.reduceByKey(lambda x, y: x + y)
# count is TransformedDStream object which exposes 

# pprint() in the Python API (scala has .print()
# It prints the first ten elements of every batch of data in a DStream 
# on the driver node running the streaming application.
# This is useful for development and debugging.
word_count.pprint()
#word_count.foreachRDD(lambda r: print(r))

ssc.start()            
ssc.awaitTermination() 


