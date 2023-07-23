import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trusted Accelerometer
TrustedAccelerometer_node1690119568488 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="TrustedAccelerometer_node1690119568488",
)

# Script generated for node Trusted Step Trainer
TrustedStepTrainer_node1690119567907 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="TrustedStepTrainer_node1690119567907",
)

# Script generated for node Join
Join_node1690119876712 = Join.apply(
    frame1=TrustedAccelerometer_node1690119568488,
    frame2=TrustedStepTrainer_node1690119567907,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1690119876712",
)

# Script generated for node Drop Fields
DropFields_node1690119906984 = DropFields.apply(
    frame=Join_node1690119876712,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1690119906984",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690120153280 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1690119906984,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="AWSGlueDataCatalog_node1690120153280",
)

job.commit()
