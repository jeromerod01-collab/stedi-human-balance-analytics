"""customer_landing_to_trusted.py
Landing -> Trusted: keep only customers who agreed to share data for research
(drop rows where shareWithResearchAsOfDate is blank). Creates stedi.customer_trusted.
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

# Source: Amazon S3 - customer landing zone
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{BUCKET}/customer/landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node",
)

# Transform - SQL Query: PrivacyFilter (drop customers without research consent)
PrivacyFilter_query = """
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
  AND shareWithResearchAsOfDate <> 0
"""
PrivacyFilter_node = sparkSqlQuery(
    glueContext,
    query=PrivacyFilter_query,
    mapping={"customer_landing": CustomerLanding_node},
    transformation_ctx="PrivacyFilter_node",
)

# Target: Amazon S3 + Data Catalog
# "Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions"
CustomerTrusted_node = glueContext.getSink(
    path=f"{BUCKET}/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node",
)
CustomerTrusted_node.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName="customer_trusted")
CustomerTrusted_node.setFormat("json")
CustomerTrusted_node.writeFrame(PrivacyFilter_node)

job.commit()
