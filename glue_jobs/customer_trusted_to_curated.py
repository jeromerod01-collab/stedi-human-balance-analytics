"""customer_trusted_to_curated.py
Trusted -> Curated: customers who agreed to share data AND have accelerometer data
(inner join customer_trusted with accelerometer_trusted on email). Output has customer
columns only, one row per customer. Creates stedi.customer_curated.
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

# Source: Data Catalog - customer_trusted
CustomerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node",
)

# Source: Data Catalog - accelerometer_trusted
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# Transform - SQL Query (two parents): inner join on email, customer columns only.
# DISTINCT collapses the many accelerometer rows per customer back to one row each.
JoinAccelerometer_query = """
SELECT DISTINCT
    c.customername,
    c.email,
    c.phone,
    c.birthday,
    c.serialnumber,
    c.registrationdate,
    c.lastupdatedate,
    c.sharewithresearchasofdate,
    c.sharewithpublicasofdate,
    c.sharewithfriendsasofdate
FROM customer_trusted c
JOIN accelerometer_trusted a
  ON c.email = a.user
"""
JoinAccelerometer_node = sparkSqlQuery(
    glueContext,
    query=JoinAccelerometer_query,
    mapping={
        "customer_trusted": CustomerTrusted_node,
        "accelerometer_trusted": AccelerometerTrusted_node,
    },
    transformation_ctx="JoinAccelerometer_node",
)

# Target: Amazon S3 + Data Catalog (create table / update schema on subsequent runs)
CustomerCurated_node = glueContext.getSink(
    path=f"{BUCKET}/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node",
)
CustomerCurated_node.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName="customer_curated")
CustomerCurated_node.setFormat("json")
CustomerCurated_node.writeFrame(JoinAccelerometer_node)

job.commit()
