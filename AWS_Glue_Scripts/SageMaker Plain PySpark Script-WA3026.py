import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

orders = spark.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", "\t") \
    .load('s3://webage-data-sets/glue-data-sets/order.csv')

orders.printSchema()

type(orders)

orders.columns

orders.count()

orders.select (['Order ID', 'Shipping Fee']).where (orders["Shipping Fee"] > 75).show()



from awsglue.dynamicframe import DynamicFrame
orders_dyf = DynamicFrame.fromDF(orders, glueContext, "orders_dyf")
glueContext.write_dynamic_frame.from_options(frame = orders_dyf,
          connection_type = "s3",
          connection_options = {"path": "s3://s158/output/"},
          format = "csv")
          
          