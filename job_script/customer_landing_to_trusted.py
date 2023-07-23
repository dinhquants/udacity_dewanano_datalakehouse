import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://dinh-nda/customers/landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Filter
Filter_node1690074454670 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1690074454670",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1690074807431 = DynamicFrame.fromDF(
    Filter_node1690074454670.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1690074807431",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690075512164 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropDuplicates_node1690074807431,
    database="stedi",
    table_name="customer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1690075512164",
)

job.commit()
