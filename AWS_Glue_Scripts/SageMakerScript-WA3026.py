import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

orders = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="order_csv")

type(orders)      

# <class 'awsglue.dynamicframe.DynamicFrame'>
#   
orders.printSchema()

orders.count()

orders.toDF().show()

orders.select_fields(['order id', 'employee id', 'customer id', 'order summary']).toDF().show(5)

orders.rename_field("`payment type`", "pmtt").toDF().columns

order_details.select_fields(['ship city']).toDF().distinct().show()


order_details = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="order_details_csv")


#order_details.select(['product_name']).distinct().show()

splitted_dyf = SplitRows.apply(order_details, { 'unit price': {'>' : 20.0}}, 'premium', 'regular') 
splitted_dyf['regular'].toDF().show(5)
 

dyf_joined = Join.apply(order_details, orders, 'order id',  'order id')

dyf_joined.count()

dyf_joined.toDF().columns

dyf_joined.printSchema()


premium_dyf = Filter.apply (dyf_joined, lambda dr: dr['unit price'] > 20.0 )
premium_dyf.toDF().select(['order id', 'unit price']).show(7)

# For three-way JOIN
employees = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="employee_csv")

three_way_join_dyf = Join.apply(order_details,
       Join.apply(orders, employees, 'employee id', 'employee id'),
      'order id', 'order id')

three_way_join_dyf.printSchema()


glueContext.write_dynamic_frame.from_options(frame = three_way_join_dyf,
          connection_type = "s3",
          connection_options = {"path": "s3://s158/output/"},
          format = "csv")
          
# Format Options for ETL Inputs and Outputs in AWS Glue
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html          
# Popular formats: "csv", "json", "parquet", "avro"             

# # run-<time stamp>-part-r-00000      in S3  

print ('Done in Glue Script ...')










      