"""step_trainer_trusted.py
Landing -> Trusted: Step Trainer readings for customers in customer_curated
(inner join on serialNumber). Output has step trainer columns only.
Creates stedi.step_trainer_trusted.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """Glue Studio "Transform - SQL Query" node."""
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

BUCKET = "s3://stedi-lakehouse-84164"
DATABASE = "stedi"

# Source: Amazon S3 - step trainer landing zone
StepTrainerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{BUCKET}/step_trainer/landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node",
)

# Source: Data Catalog - customer_curated
CustomerCurated_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node",
)

# Transform - SQL Query (two parents): inner join on serial number, step trainer columns only
JoinCustomerCurated_query = """
SELECT s.sensorReadingTime, s.serialNumber, s.distanceFromObject
FROM step_trainer_landing s
JOIN customer_curated c
  ON s.serialNumber = c.serialnumber
"""
JoinCustomerCurated_node = sparkSqlQuery(
    glueContext,
    query=JoinCustomerCurated_query,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node,
        "customer_curated": CustomerCurated_node,
    },
    transformation_ctx="JoinCustomerCurated_node",
)

# Target: Amazon S3 + Data Catalog (create table / update schema on subsequent runs)
StepTrainerTrusted_node = glueContext.getSink(
    path=f"{BUCKET}/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node",
)
StepTrainerTrusted_node.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node.setFormat("json")
StepTrainerTrusted_node.writeFrame(JoinCustomerCurated_node)

job.commit()
