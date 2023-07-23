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

# Script generated for node Customer Trusted
CustomerTrusted_node1690077483083 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1690077483083",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dinh-nda/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join
Join_node1690077575117 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1690077483083,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690077575117",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690077635684 = glueContext.write_dynamic_frame.from_catalog(
    frame=Join_node1690077575117,
    database="stedi",
    table_name="accelerometer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1690077635684",
)

job.commit()
