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

# Script generated for node Curated Customer
CuratedCustomer_node1690117687908 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CuratedCustomer_node1690117687908",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1690117716315 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dinh-nda/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1690117716315",
)

# Script generated for node Join
Join_node1690117916551 = Join.apply(
    frame1=CuratedCustomer_node1690117687908,
    frame2=StepTrainerLanding_node1690117716315,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1690117916551",
)

# Script generated for node Drop Fields
DropFields_node1690117997583 = DropFields.apply(
    frame=Join_node1690117916551,
    paths=[
        "customername",
        "email",
        "phone",
        "serialnumber",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1690117997583",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690118053780 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1690117997583,
    database="stedi",
    table_name="step_trainer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1690118053780",
)

job.commit()
