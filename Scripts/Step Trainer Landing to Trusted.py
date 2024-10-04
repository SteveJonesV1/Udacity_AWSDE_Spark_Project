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

# Script generated for node Customers Curated
CustomersCurated_node1728050660058 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="CustomersCurated_node1728050660058")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1728050658466 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1728050658466")

# Script generated for node Join and drop fields
SqlQuery4595 = '''
select step_landing.sensorreadingtime,step_landing.serialnumber,step_landing.distancefromobject 
from step_landing inner join customer_curated
on step_landing.serialnumber = customer_curated.serialnumber

'''
Joinanddropfields_node1728050901564 = sparkSqlQuery(glueContext, query = SqlQuery4595, mapping = {"step_landing":StepTrainerLanding_node1728050658466, "customer_curated":CustomersCurated_node1728050660058}, transformation_ctx = "Joinanddropfields_node1728050901564")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1728050869156 = glueContext.getSink(path="s3://steveudacityspark/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1728050869156")
StepTrainerTrusted_node1728050869156.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1728050869156.setFormat("json")
StepTrainerTrusted_node1728050869156.writeFrame(Joinanddropfields_node1728050901564)
job.commit()