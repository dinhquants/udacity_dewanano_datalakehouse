import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trusted Customer
TrustedCustomer_node1690083458417 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="TrustedCustomer_node1690083458417",
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
Join_node1690083511459 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=TrustedCustomer_node1690083458417,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690083511459",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource 
where timestamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1690083568363 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1690083511459},
    transformation_ctx="SQLQuery_node1690083568363",
)

# Script generated for node Drop Fields
DropFields_node1690083594608 = DropFields.apply(
    frame=SQLQuery_node1690083568363,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1690083594608",
)

# Script generated for node Drop Duplicates Email
DropDuplicatesEmail_node1690116269998 = DynamicFrame.fromDF(
    DropFields_node1690083594608.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicatesEmail_node1690116269998",
)

# Script generated for node Curated Customer
CuratedCustomer_node1690114899090 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropDuplicatesEmail_node1690116269998,
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CuratedCustomer_node1690114899090",
)

job.commit()
