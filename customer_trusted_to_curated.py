import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1713112037775 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometertrusted_node1713112037775",
    )
)

# Script generated for node customer trusted
customertrusted_node1713112060619 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="customer_trusted",
        transformation_ctx="customertrusted_node1713112060619",
    )
)

# Script generated for node Join
customertrusted_node1713112060619DF = customertrusted_node1713112060619.toDF()
accelerometertrusted_node1713112037775DF = (
    accelerometertrusted_node1713112037775.toDF()
)
Join_node1713115287681 = DynamicFrame.fromDF(
    customertrusted_node1713112060619DF.join(
        accelerometertrusted_node1713112037775DF,
        (
            customertrusted_node1713112060619DF["email"]
            == accelerometertrusted_node1713112037775DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1713115287681",
)

# Script generated for node customer curated
customercurated_node1713112067187 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=Join_node1713115287681,
        database="mjtravers",
        table_name="customer_curated",
        transformation_ctx="customercurated_node1713112067187",
    )
)

job.commit()
