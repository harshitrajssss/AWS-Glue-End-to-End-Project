import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1719182573074 = glueContext.create_dynamic_frame.from_catalog(database="glue", table_name="hinputpracticalbucket", transformation_ctx="AmazonS3_node1719182573074")

# Script generated for node Select Fields
SelectFields_node1719182691569 = SelectFields.apply(frame=AmazonS3_node1719182573074, paths=["col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8"], transformation_ctx="SelectFields_node1719182691569")

# Script generated for node Drop Fields
DropFields_node1719182808794 = DropFields.apply(frame=SelectFields_node1719182691569, paths=["col7"], transformation_ctx="DropFields_node1719182808794")

# Script generated for node Amazon S3
AmazonS3_node1719182851262 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1719182808794, connection_type="s3", format="avro", connection_options={"path": "s3://houtputpracticalbucket", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1719182851262")

job.commit()