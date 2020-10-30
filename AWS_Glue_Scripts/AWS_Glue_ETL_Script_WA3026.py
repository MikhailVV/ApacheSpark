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


order_details = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="order_details_csv")


employees = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="employee_csv")


customers = glueContext.create_dynamic_frame.from_catalog(
             database="sx_db",
             table_name="customer_csv")
             
four_way_join_dyf = Join.apply(order_details,
      Join.apply(customers,
      Join.apply(orders, employees, 'employee id', 'employee id'),
      'customer id', 'customer id'),
      'order id', 'order id')

dyf_out = four_way_join_dyf.select_fields(['order id', 'order detail id', 'employee id', 'customer id']) 

glueContext.write_dynamic_frame.from_options(frame = dyf_out,
          connection_type = "s3",
          connection_options = {"path": "s3://s158/output/"},
          format = "csv")
          











      