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

# Script generated for node accelerometrer trusted
accelerometrertrusted_node1713116286747 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometrertrusted_node1713116286747",
    )
)

# Script generated for node step  trainer trusted
steptrainertrusted_node1713116288353 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="step_trainer_trusted",
        transformation_ctx="steptrainertrusted_node1713116288353",
    )
)

# Script generated for node customer trusted
customertrusted_node1713220065141 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="customer_trusted",
        transformation_ctx="customertrusted_node1713220065141",
    )
)

# Script generated for node SQL Query
SqlQuery0 = """
select s.*, a.* from s join a on s.sensorReadingTime = a.timeStamp
join c on a.user = c.email join c as c2 on s.seriaLnumber = c2.serialnumber
"""
SQLQuery_node1713220072074 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "c": customertrusted_node1713220065141,
        "s": steptrainertrusted_node1713116288353,
        "a": accelerometrertrusted_node1713116286747,
    },
    transformation_ctx="SQLQuery_node1713220072074",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713220451679 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=SQLQuery_node1713220072074,
        database="mjtravers",
        table_name="machine_learning_curated",
        transformation_ctx="AWSGlueDataCatalog_node1713220451679",
    )
)

job.commit()
