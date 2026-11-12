"""accelerometer_landing_to_trusted.py
Landing -> Trusted: keep only accelerometer readings from customers in customer_trusted
(inner join on email). Output has accelerometer columns only. Creates stedi.accelerometer_trusted.
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

# Source: Amazon S3 - accelerometer landing zone
AccelerometerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{BUCKET}/accelerometer/landing/"], "recurse": True},
    transformation_ctx="AccelerometerLanding_node",
)

# Source: Data Catalog - customer_trusted
CustomerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node",
)

# Transform - SQL Query (two parents): inner join on email, keep accelerometer columns only
JoinCustomer_query = """
SELECT a.user, a.timestamp, a.x, a.y, a.z
FROM accelerometer_landing a
JOIN customer_trusted c
  ON a.user = c.email
"""
JoinCustomer_node = sparkSqlQuery(
    glueContext,
    query=JoinCustomer_query,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node,
        "customer_trusted": CustomerTrusted_node,
    },
    transformation_ctx="JoinCustomer_node",
)

# Target: Amazon S3 + Data Catalog (create table / update schema on subsequent runs)
AccelerometerTrusted_node = glueContext.getSink(
    path=f"{BUCKET}/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node",
)
AccelerometerTrusted_node.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node.setFormat("json")
AccelerometerTrusted_node.writeFrame(JoinCustomer_node)

job.commit()
