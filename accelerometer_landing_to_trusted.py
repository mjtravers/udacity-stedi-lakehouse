import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(
    glueContext, query, mapping, transformation_ctx
) -> DynamicFrame:
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

# Script generated for node accelerometer landing
accelerometerlanding_node1713110176265 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="accelerometer_landing",
        transformation_ctx="accelerometerlanding_node1713110176265",
    )
)

# Script generated for node customer trusted
customertrusted_node1713110174320 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="customer_trusted",
        transformation_ctx="customertrusted_node1713110174320",
    )
)

# Script generated for node SQL Query
SqlQuery0 = """
select a.user, a.timestamp, a.x, a.y, a.z from a
where a.user in (select email from c);

"""
SQLQuery_node1713110182722 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "a": accelerometerlanding_node1713110176265,
        "c": customertrusted_node1713110174320,
    },
    transformation_ctx="SQLQuery_node1713110182722",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713110185183 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=SQLQuery_node1713110182722,
        database="mjtravers",
        table_name="accelerometer_trusted",
        transformation_ctx="AWSGlueDataCatalog_node1713110185183",
    )
)

job.commit()
