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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1728051718747 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1728051718747")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1728051720331 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1728051720331")

# Script generated for node Join and drop
SqlQuery4682 = '''
select step.serialnumber
,step.sensorreadingtime
,step.distancefromobject 
,accel.x
,accel.y
,accel.z
from accel inner join step
on step.sensorreadingtime = accel.timestamp
'''
Joinanddrop_node1728053036817 = sparkSqlQuery(glueContext, query = SqlQuery4682, mapping = {"accel":AccelerometerTrusted_node1728051720331, "step":StepTrainerTrusted_node1728051718747}, transformation_ctx = "Joinanddrop_node1728053036817")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1728053183912 = glueContext.getSink(path="s3://steveudacityspark/machinelearning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1728053183912")
MachineLearningCurated_node1728053183912.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1728053183912.setFormat("json")
MachineLearningCurated_node1728053183912.writeFrame(Joinanddrop_node1728053036817)
job.commit()