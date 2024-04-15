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

# Script generated for node customer trusted
customertrusted_node1713115850640 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="customer_trusted",
        transformation_ctx="customertrusted_node1713115850640",
    )
)

# Script generated for node step trainer landing
steptrainerlanding_node1713115882128 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="mjtravers",
        table_name="step_trainer_landing",
        transformation_ctx="steptrainerlanding_node1713115882128",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1713115991782 = ApplyMapping.apply(
    frame=customertrusted_node1713115850640,
    mappings=[
        ("customername", "string", "right_customername", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        (
            "sharewithpublicasofdate",
            "long",
            "right_sharewithpublicasofdate",
            "long",
        ),
        (
            "sharewithfriendsasofdate",
            "long",
            "right_sharewithfriendsasofdate",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1713115991782",
)

# Script generated for node Join
steptrainerlanding_node1713115882128DF = (
    steptrainerlanding_node1713115882128.toDF()
)
RenamedkeysforJoin_node1713115991782DF = (
    RenamedkeysforJoin_node1713115991782.toDF()
)
Join_node1713115896846 = DynamicFrame.fromDF(
    steptrainerlanding_node1713115882128DF.join(
        RenamedkeysforJoin_node1713115991782DF,
        (
            steptrainerlanding_node1713115882128DF["serialnumber"]
            == RenamedkeysforJoin_node1713115991782DF["right_serialnumber"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1713115896846",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1713115901313 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=Join_node1713115896846,
        database="mjtravers",
        table_name="step_trainer_trusted",
        transformation_ctx="steptrainertrusted_node1713115901313",
    )
)

job.commit()
