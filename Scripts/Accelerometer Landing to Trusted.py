import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1727964516255 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1727964516255")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1727964472022 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1727964472022")

# Script generated for node Join
Join_node1727964552687 = Join.apply(frame1=AccelerometerLanding_node1727964472022, frame2=CustomerTrusted_node1727964516255, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1727964552687")

# Script generated for node Drop Fields
SqlQuery4471 = '''
select user,timestamp,x,y,z from myDataSource
order by user,timestamp
'''
DropFields_node1727964860526 = sparkSqlQuery(glueContext, query = SqlQuery4471, mapping = {"myDataSource":Join_node1727964552687}, transformation_ctx = "DropFields_node1727964860526")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1727964578623 = glueContext.getSink(path="s3://steveudacityspark/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1727964578623")
AccelerometerTrusted_node1727964578623.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1727964578623.setFormat("json")
AccelerometerTrusted_node1727964578623.writeFrame(DropFields_node1727964860526)
job.commit()